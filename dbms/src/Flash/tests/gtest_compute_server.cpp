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

#include <Common/FailPoint.h>
#include <TestUtils/MPPTaskTestUtils.h>

#include <chrono>
#include <cstddef>
#include <optional>
#include <thread>

#include "Common/Exception.h"
#include "TestUtils/executorSerializer.h"

namespace DB
{
namespace FailPoints
{
extern const char random_aggregate_create_state_failpoint[];
extern const char random_aggregate_merge_failpoint[];
} // namespace FailPoints
namespace tests
{

class MockTsGenerator : public ext::Singleton<MockTsGenerator>
{
public:
    Int64 nextTs()
    {
        return ++current_ts;
    }

private:
    std::atomic<UInt64> current_ts = 0;
};
class ComputeServerRunner : public DB::tests::MPPTaskTestUtils
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
        /// for agg
        context.addMockTable(
            {"test_db", "test_table_1"},
            {{"s1", TiDB::TP::TypeLong}, {"s2", TiDB::TP::TypeString}, {"s3", TiDB::TP::TypeString}},
            {toNullableVec<Int32>("s1", {1, {}, 10000000, 10000000}), toNullableVec<String>("s2", {"apple", {}, "banana", "test"}), toNullableVec<String>("s3", {"apple", {}, "banana", "test"})});

        /// for join
        context.addMockTable(
            {"test_db", "l_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});
        context.addMockTable(
            {"test_db", "r_table"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", {"banana", {}, "banana"}), toNullableVec<String>("join_c", {"apple", {}, "banana"})});

        std::vector<std::optional<String>> col1;
        for (size_t i = 0; i < 1000000; i++)
        {
            col1.push_back("appple");
        }
        context.addMockTable(
            {"test_db", "l_table_1"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", col1), toNullableVec<String>("join_c", col1)});
        context.addMockTable(
            {"test_db", "r_table_1"},
            {{"s", TiDB::TP::TypeString}, {"join_c", TiDB::TP::TypeString}},
            {toNullableVec<String>("s", col1), toNullableVec<String>("join_c", col1)});
    }
};

TEST_F(ComputeServerRunner, runAggTasks)
try
{
    startServers({"0.0.0.0:3930", "0.0.0.0:3931", "0.0.0.0:3932", "0.0.0.0:3933"});
    fiu_init(0);
    fiu_enable_random(FailPoints::random_aggregate_create_state_failpoint, 1, nullptr, 0, 1);
    // fiu_enable_random(FailPoints::random_aggregate_merge_failpoint, 1, nullptr, 0, 1);

    {
        //         auto tasks = context.scan("test_db", "test_table_1")
        //                          .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
        //                          .project({"max(s1)"})
        //                          .buildMPPTasks(context, serverNum());
        //         std::vector<String> expected_strings = {
        //             R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
        //  aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
        //   table_scan_0 | {<0, Long>, <1, String>, <2, String>}
        // )",
        //             R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
        //  aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
        //   table_scan_0 | {<0, Long>, <1, String>, <2, String>}
        // )",
        //             R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
        //  aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
        //   table_scan_0 | {<0, Long>, <1, String>, <2, String>}
        // )",
        //             R"(exchange_sender_5 | type:Hash, {<0, Long>, <1, String>, <2, String>}
        //  aggregation_4 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
        //   table_scan_0 | {<0, Long>, <1, String>, <2, String>}
        // )",
        //             R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
        //  project_2 | {<0, Long>}
        //   aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
        //    exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
        // )",
        //             R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
        //  project_2 | {<0, Long>}
        //   aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
        //    exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
        // )",
        //             R"(
        // exchange_sender_3 | type:PassThrough, {<0, Long>}
        //  project_2 | {<0, Long>}
        //   aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
        //    exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
        // )",
        //             R"(exchange_sender_3 | type:PassThrough, {<0, Long>}
        //  project_2 | {<0, Long>}
        //   aggregation_1 | group_by: {<1, String>, <2, String>}, agg_func: {max(<0, Long>)}
        //    exchange_receiver_6 | type:PassThrough, {<0, Long>, <1, String>, <2, String>}
        // )"};
        //         size_t task_size = tasks.size();
        //         for (size_t i = 0; i < task_size; ++i)
        //         {
        //             ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
        //         }

        // auto expected_cols = {toNullableVec<Int32>({1, {}, 10000000, 10000000})};
        WRAP_FOR_DIS_ENABLE_PLANNER_BEGIN

        MockComputeServerManager::instance().resetMockMPPServerInfo(1);
        // auto tasks = context
        //                  .scan("test_db", "l_table")
        //                  .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
        //                  .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
        //                  .project({col("max(l_table.s)"), col("l_table.s")})
        //                  .buildMPPTasks(context, 1);
        // std::cout << "ywq test splitted tasks" << std::endl;
        // for (auto task : tasks)
        // {
        //     std::cout << ExecutorSerializer().serialize(task.dag_request.get()) << std::endl;
        // }
        TiFlashTestEnv::getGlobalContext().setMPPTest();
        MockComputeServerManager::instance().setMockStorage(context.mockStorage());

        auto run_agg = [&] {
            try
            {
                std::cout << "ywq test called..." << std::endl;
                DAGProperties properties;
                // enable mpp
                properties.is_mpp_query = true;
                properties.mpp_partition_num = 1;
                properties.start_ts = MockTsGenerator::instance().nextTs();

                // auto tasks = context.scan("test_db", "test_table_1")
                //                  .aggregation({Max(col("s1"))}, {col("s2"), col("s3")})
                //                  .project({"max(s1)"})
                //                  .buildMPPTasks(context, properties);
                // auto expected_cols = {toNullableVec<Int32>({1, {}, 10000000, 10000000})};
                // ASSERT_COLUMNS_EQ_UR(executeMPPTasks(tasks, properties, MockComputeServerManager::instance().getServerConfigMap()), expected_cols);


                auto tasks = context
                                 .scan("test_db", "l_table_1")
                                 .join(context.scan("test_db", "r_table_1"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
                                 .aggregation({Max(col("l_table.s"))}, {col("l_table.s")})
                                 .project({col("max(l_table.s)"), col("l_table.s")})
                                 .buildMPPTasks(context, properties);

                size_t task_size = tasks.size();
                for (size_t i = 0; i < task_size; ++i)
                {
                    std::cout << ExecutorSerializer().serialize(tasks[i].dag_request.get()) << std::endl;
                }
                executeMPPTasks(tasks, properties, MockComputeServerManager::instance().getServerConfigMap());
            }
            catch (Exception & e)
            {
                std::cout << e.getStackTrace().toString() << std::endl;
            }
        };
        // std::thread thd(run_agg);
        // thd.join();
        std::vector<std::thread> threads;
        for (size_t j = 0; j < 10; j++)
        {
            threads.push_back(std::thread(run_agg));
        }
        for (size_t j = 0; j < 10; ++j)
        {
            threads[j].join();
        }
        WRAP_FOR_DIS_ENABLE_PLANNER_END
    }

    // std::this_thread::sleep_for(std::chrono::seconds(10));

    //     {
    //         auto tasks = context.scan("test_db", "test_table_1")
    //                          .aggregation({Count(col("s1"))}, {})
    //                          .project({"count(s1)"})
    //                          .buildMPPTasks(context, serverNum());

    //         std::vector<String> expected_strings = {
    //             R"(exchange_sender_5 | type:PassThrough, {<0, Longlong>}
    //  aggregation_4 | group_by: {}, agg_func: {count(<0, Long>)}
    //   table_scan_0 | {<0, Long>}
    //             )",
    //             R"(exchange_sender_5 | type:PassThrough, {<0, Longlong>}
    //  aggregation_4 | group_by: {}, agg_func: {count(<0, Long>)}
    //   table_scan_0 | {<0, Long>}
    //             )",
    //             R"(exchange_sender_5 | type:PassThrough, {<0, Longlong>}
    //  aggregation_4 | group_by: {}, agg_func: {count(<0, Long>)}
    //   table_scan_0 | {<0, Long>}
    //             )",
    //             R"(exchange_sender_5 | type:PassThrough, {<0, Longlong>}
    //  aggregation_4 | group_by: {}, agg_func: {count(<0, Long>)}
    //   table_scan_0 | {<0, Long>}
    //             )",
    //             R"(exchange_sender_3 | type:PassThrough, {<0, Longlong>}
    //  project_2 | {<0, Longlong>}
    //   aggregation_1 | group_by: {}, agg_func: {sum(<0, Longlong>)}
    //    exchange_receiver_6 | type:PassThrough, {<0, Longlong>})"};

    //         size_t task_size = tasks.size();
    //         for (size_t i = 0; i < task_size; ++i)
    //         {
    //             ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
    //         }

    //         auto expected_cols = {toVec<UInt64>({3})};
    //         ASSERT_MPPTASK_EQUAL_WITH_SERVER_NUM(
    //             context.scan("test_db", "test_table_1").aggregation({Count(col("s1"))}, {}).project({"count(s1)"}),
    //             expect_cols);
    //     }
}
CATCH

// TEST_F(ComputeServerRunner, runJoinTasks)
// try
// {
//     startServers({"0.0.0.0:3930", "0.0.0.0:3931", "0.0.0.0:3932"});
//     {
//         auto tasks = context
//                          .scan("test_db", "l_table")
//                          .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
//                          .buildMPPTasks(context, serverNum());

//         auto expected_cols = {
//             toNullableVec<String>({{}, "banana", "banana"}),
//             toNullableVec<String>({{}, "apple", "banana"}),
//             toNullableVec<String>({{}, "banana", "banana"}),
//             toNullableVec<String>({{}, "apple", "banana"})};

//         std::vector<String> expected_strings = {
//             R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
//  table_scan_1 | {<0, String>, <1, String>})",
//             R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
//  table_scan_1 | {<0, String>, <1, String>})",
//             R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
//  table_scan_1 | {<0, String>, <1, String>})",
//             R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
//  table_scan_0 | {<0, String>, <1, String>})",
//             R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
//  table_scan_0 | {<0, String>, <1, String>})",
//             R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
//  table_scan_0 | {<0, String>, <1, String>})",
//             R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
//  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
//   exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
//   exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})",
//             R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
//  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
//   exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
//   exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})",
//             R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
//  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
//   exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
//   exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})"};

//         size_t task_size = tasks.size();
//         for (size_t i = 0; i < task_size; ++i)
//         {
//             ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
//         }

//         ASSERT_MPPTASK_EQUAL_WITH_SERVER_NUM(
//             context
//                 .scan("test_db", "l_table")
//                 .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")}),
//             expect_cols);
//     }
//     {
//         auto tasks = context
//                          .scan("test_db", "l_table")
//                          .join(context.scan("test_db", "r_table"), tipb::JoinType::TypeLeftOuterJoin, {col("join_c")})
//                          .buildMPPTasks(context, 1);

//         std::vector<String> expected_strings = {
//             R"(exchange_sender_5 | type:Hash, {<0, String>, <1, String>}
//  table_scan_1 | {<0, String>, <1, String>})",
//             R"(exchange_sender_4 | type:Hash, {<0, String>, <1, String>}
//  table_scan_0 | {<0, String>, <1, String>})",
//             R"(exchange_sender_3 | type:PassThrough, {<0, String>, <1, String>, <2, String>, <3, String>}
//  Join_2 | LeftOuterJoin, HashJoin. left_join_keys: {<0, String>}, right_join_keys: {<0, String>}
//   exchange_receiver_6 | type:PassThrough, {<0, String>, <1, String>}
//   exchange_receiver_7 | type:PassThrough, {<0, String>, <1, String>})"};

//         size_t task_size = tasks.size();
//         for (size_t i = 0; i < task_size; ++i)
//         {
//             ASSERT_DAGREQUEST_EQAUL(expected_strings[i], tasks[i].dag_request);
//         }
//     }
// }
// CATCH

} // namespace tests
} // namespace DB
