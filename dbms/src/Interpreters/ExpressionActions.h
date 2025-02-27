// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Interpreters/Settings.h>
#include <Storages/Transaction/Collator.h>

#include <unordered_map>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using NameWithAlias = std::pair<std::string, std::string>;
using NamesWithAliases = std::vector<NameWithAlias>;

class Join;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

class IFunctionBuilder;
using FunctionBuilderPtr = std::shared_ptr<IFunctionBuilder>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;


/** Action on the block.
  */
struct ExpressionAction
{
public:
    enum Type
    {
        ADD_COLUMN,
        REMOVE_COLUMN,
        COPY_COLUMN,

        APPLY_FUNCTION,

        JOIN,

        /// Reorder and rename the columns, delete the extra ones. The same column names are allowed in the result.
        PROJECT,
    };

    Type type;

    /// For ADD/REMOVE/COPY_COLUMN.
    std::string source_name;
    std::string result_name;
    DataTypePtr result_type;

    /// For ADD_COLUMN.
    ColumnPtr added_column;

    /// For APPLY_FUNCTION.
    FunctionBuilderPtr function_builder;
    FunctionBasePtr function;
    Names argument_names;
    TiDB::TiDBCollatorPtr collator = nullptr;

    /// For JOIN
    std::shared_ptr<const Join> join;
    NamesAndTypesList columns_added_by_join;

    /// For PROJECT.
    NamesWithAliases projections;

    /// If result_name_ == "", as name "function_name(arguments separated by commas) is used".
    static ExpressionAction applyFunction(
        const FunctionBuilderPtr & function_,
        const std::vector<std::string> & argument_names_,
        std::string result_name_ = "",
        const TiDB::TiDBCollatorPtr & collator_ = nullptr);

    static ExpressionAction addColumn(const ColumnWithTypeAndName & added_column_);
    static ExpressionAction removeColumn(const std::string & removed_name);
    static ExpressionAction copyColumn(const std::string & from_name, const std::string & to_name);
    static ExpressionAction project(const NamesWithAliases & projected_columns_);
    static ExpressionAction project(const Names & projected_columns_);
    static ExpressionAction ordinaryJoin(std::shared_ptr<const Join> join_, const NamesAndTypesList & columns_added_by_join_);

    /// Which columns necessary to perform this action.
    Names getNeededColumns() const;

    std::string toString() const;

private:
    friend class ExpressionActions;

    void prepare(Block & sample_block);
    void execute(Block & block) const;
    void executeOnTotals(Block & block) const;
};


/** Contains a sequence of actions on the block.
  */
class ExpressionActions
{
public:
    using Actions = std::vector<ExpressionAction>;

    ExpressionActions(const NamesAndTypesList & input_columns_, const Settings & settings_)
        : input_columns(input_columns_)
        , settings(settings_)
    {
        for (const auto & input_elem : input_columns)
            sample_block.insert(ColumnWithTypeAndName(nullptr, input_elem.type, input_elem.name));
    }

    ExpressionActions(const NamesAndTypes & input_columns_, const Settings & settings_)
        : input_columns(input_columns_.cbegin(), input_columns_.cend())
        , settings(settings_)
    {
        for (const auto & input_elem : input_columns)
            sample_block.insert(ColumnWithTypeAndName(nullptr, input_elem.type, input_elem.name));
    }

    /// For constant columns the columns themselves can be contained in `input_columns_`.
    ExpressionActions(const ColumnsWithTypeAndName & input_columns_, const Settings & settings_)
        : settings(settings_)
    {
        for (const auto & input_elem : input_columns_)
        {
            input_columns.emplace_back(input_elem.name, input_elem.type);
            sample_block.insert(input_elem);
        }
    }

    /// Add the input column.
    /// The name of the column must not match the names of the intermediate columns that occur when evaluating the expression.
    /// The expression must not have any PROJECT actions.
    void addInput(const ColumnWithTypeAndName & column);
    void addInput(const NameAndTypePair & column);

    void add(const ExpressionAction & action);

    /// Adds new column names to out_new_columns (formed as a result of the added action).
    void add(const ExpressionAction & action, Names & out_new_columns);

    /// Adds to the beginning the removal of all extra columns.
    void prependProjectInput();

    /// - Adds actions to delete all but the specified columns.
    /// - Removes unused input columns.
    /// - Can somehow optimize the expression.
    /// - Does not reorder the columns.
    /// - Does not remove "unexpected" columns (for example, added by functions).
    /// - If output_columns is empty, leaves one arbitrary column (so that the number of rows in the block is not lost).
    void finalize(const Names & output_columns);

    const Actions & getActions() const { return actions; }

    /// Get a list of input columns.
    Names getRequiredColumns() const
    {
        Names names;
        for (const auto & input_column : input_columns)
            names.push_back(input_column.name);
        return names;
    }

    const NamesAndTypesList & getRequiredColumnsWithTypes() const { return input_columns; }

    /// Execute the expression on the block. The block must contain all the columns returned by getRequiredColumns.
    void execute(Block & block) const;

    /** Execute the expression on the block of total values.
      * Almost the same as `execute`. The difference is only when JOIN is executed.
      */
    void executeOnTotals(Block & block) const;

    /// Obtain a sample block that contains the names and types of result columns.
    const Block & getSampleBlock() const { return sample_block; }

    std::string dumpActions() const;

    static std::string getSmallestColumn(const NamesAndTypesList & columns);

    BlockInputStreamPtr createStreamWithNonJoinedDataIfFullOrRightJoin(const Block & source_header, size_t index, size_t step, size_t max_block_size) const;

private:
    NamesAndTypesList input_columns;
    Actions actions;
    Block sample_block;
    Settings settings;

    void checkLimits(Block & block) const;

    void addImpl(ExpressionAction action, Names & new_names);
};

using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;


/** The sequence of transformations over the block.
  * It is assumed that the result of each step is fed to the input of the next step.
  * Used to execute parts of the query individually.
  *
  * For example, you can create a chain of two steps:
  *     1) evaluate the expression in the WHERE clause,
  *     2) calculate the expression in the SELECT section,
  * and between the two steps do the filtering by value in the WHERE clause.
  */
struct ExpressionActionsChain
{
    struct Step
    {
        ExpressionActionsPtr actions;
        Names required_output;

        explicit Step(const ExpressionActionsPtr & actions_ = nullptr, const Names & required_output_ = Names())
            : actions(actions_)
            , required_output(required_output_)
        {}
    };

    using Steps = std::vector<Step>;

    Settings settings;
    Steps steps;

    void addStep();

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ExpressionActionsPtr getLastActions()
    {
        if (steps.empty())
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

        return steps.back().actions;
    }

    Step & getLastStep()
    {
        if (steps.empty())
            throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

        return steps.back();
    }

    std::string dumpChain();
};

} // namespace DB
