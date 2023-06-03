#include "lgraph/lgraph.h"

#include <omp.h>

#include <iostream>
#include <tuple>
#include <thread>
#include <future>
#include <chrono>
#include <random>

bool optimistic = false;


void Initialize(auto& db) {
    db.DropAllData();
    bool ok;
    ok = db.AddVertexLabel("Account", {
             {"id", lgraph_api::FieldType::INT64, false},
             {"name", lgraph_api::FieldType::STRING, true},
             {"balance", lgraph_api::FieldType::DOUBLE, true},
             {"numTransferred", lgraph_api::FieldType::INT64, true},
             {"transHistory", lgraph_api::FieldType::STRING, true},
             {"versionHistory", lgraph_api::FieldType::STRING, true}
         }, "id");
    if (!ok) throw std::runtime_error("failed to register vertex label Account");
    ok = db.AddEdgeLabel("transfer", {
                                      {"amount", lgraph_api::FieldType::DOUBLE, true},
                                      {"versionHistory", lgraph_api::FieldType::STRING, true}
                                  });
    if (!ok) throw std::runtime_error("failed to register edge label transfer");
}

void AppendStringToField(auto& vit, const std::string& field, const std::string& s) {
    if (vit[field].IsNull()) {
        vit.SetField(field, lgraph_api::FieldData::String(s));
    } else {
        vit.SetField(field, lgraph_api::FieldData::String(vit[field].AsString() + ";" + s));
    }
}

int64_t CountItemsInField(auto& vit, const std::string& field) {
    if (vit[field].IsNull()) return 0;
    int64_t num_items = 1;
    auto s = vit[field].AsString();
    for (auto c : s) {
        if (c == ';') num_items ++;
    }
    return num_items;
}

// Atomicity

void AtomicityInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Account",
        {
            "id", "name", "transHistory"
        },
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::String("AliceAcc"),
            lgraph_api::FieldData::String("100")
        }
    );
    txn.AddVertex(
        "Account",
        {
            "id", "name", "transHistory"
        },
        {
            lgraph_api::FieldData::Int64(2),
            lgraph_api::FieldData::String("BobAcc"),
            lgraph_api::FieldData::String("50;150")
        }
    );
    txn.Commit();
}

bool AtomicityC(auto& db, int64_t account1_id, int64_t account2_id, int64_t transHistory) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    auto p1_vid = vit.GetId();
    AppendStringToField(vit, "transHistory", std::to_string(transHistory));
    auto p2_vid = txn.AddVertex(
        "Account",
        {"id"},
        {lgraph_api::FieldData::Int64(account2_id)}
    );
    txn.AddEdge(
        p1_vid, p2_vid, "transfer",
        {"amount"},
        {lgraph_api::FieldData::Int64(transHistory)}
    );
    txn.Commit();
    return true;
}

bool AtomicityRB(auto& db, int64_t account1_id, int64_t account2_id, int64_t transHistory) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    auto p1_vid = vit.GetId();
    AppendStringToField(vit, "transHistory", std::to_string(transHistory));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account2_id) {
            txn.Abort();
            return false;
        }
    }
    auto p2_vid = txn.AddVertex(
        "Account",
        {"id"},
        {lgraph_api::FieldData::Int64(account2_id)}
    );
    txn.Commit();
    return true;
}

std::tuple<int64_t, int64_t, int64_t> AtomicityCheck(auto& db) {
    auto txn = db.CreateReadTxn();
    int64_t num_accounts = 0;
    int64_t num_names = 0;
    int64_t num_transferred = 0;
    for (auto vit = txn.GetVertexIterator(); vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() != "Account") continue;
        num_accounts ++;
        if (!vit["name"].IsNull()) num_names ++;
        num_transferred += CountItemsInField(vit, "transHistory");
    }
    return std::make_tuple(num_accounts, num_names, num_transferred);
}

void AtomicityCTest(auto& db) {
    Initialize(db);

    AtomicityInit(db);

    auto committed = AtomicityCheck(db);

    int64_t num_txns = 50;
    int64_t num_aborted_txns = 0;

#pragma omp parallel for reduction(+:num_aborted_txns)
    for (int64_t i = 0; i < num_txns; i ++) {
        bool successful;
        try {
            successful = AtomicityC(db, 1, 3 + i, 200 + i);
        } catch (std::exception& e) {
            successful = false;
        }
        if (successful) {
#pragma omp critical
            {
                std::get<0>(committed) ++;
                std::get<2>(committed) ++;
            }
        } else {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    auto finalstate = AtomicityCheck(db);

    if (committed == finalstate) {
        std::cout << "AtomicityCTest passed" << std::endl;
    } else {
        std::cout << "AtomicityCTest failed" << std::endl;
    }
}

void AtomicityRBTest(auto& db) {
    Initialize(db);

    AtomicityInit(db);

    auto committed = AtomicityCheck(db);

    int64_t num_txns = 50;
    int64_t num_aborted_txns = 0;

#pragma omp parallel for reduction(+:num_aborted_txns)
    for (int64_t i = 0; i < num_txns; i ++) {
        bool successful;
        try {
            if (i % 2 == 0) {
                successful = AtomicityRB(db, 1, 2, 200);
            } else {
                successful = AtomicityRB(db, 1, 3 + i, 200);
            }
        } catch (std::exception& e) {
            successful = false;
        }
        if (successful) {
#pragma omp critical
            {
                std::get<0>(committed) ++;
                std::get<2>(committed) ++;
            }
        } else {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    auto finalstate = AtomicityCheck(db);

    if (committed == finalstate) {
        std::cout << "AtomicityRBTest passed" << std::endl;
    } else {
        std::cout << "AtomicityRBTest failed" << std::endl;
    }
}

// Dirty Writes

void G0Init(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto p1_vid = txn.AddVertex(
        "Account",
        {"id", "versionHistory"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::String("0")
        }
    );
    auto p2_vid = txn.AddVertex(
        "Account",
        {"id", "versionHistory"},
        {
            lgraph_api::FieldData::Int64(2),
            lgraph_api::FieldData::String("0")
        }
    );
    txn.AddEdge(
        p1_vid, p2_vid, "transfer",
        {"versionHistory"},
        {lgraph_api::FieldData::String("0")}
    );
    txn.Commit();
}

void G0(auto& db, int64_t account1_id, int64_t account2_id, int64_t txn_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    auto p1_vid = vit.GetId();
    AppendStringToField(vit, "versionHistory", std::to_string(txn_id));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account2_id) break;
    }
    auto p2_vid = vit.GetId();
    AppendStringToField(vit, "versionHistory", std::to_string(txn_id));
    auto TRANSFER = txn.GetEdgeLabelId("transfer");
    auto oeit = txn.GetOutEdgeIterator(lgraph_api::EdgeUid(p1_vid, p2_vid, TRANSFER, 0, 0));
    AppendStringToField(oeit, "versionHistory", std::to_string(txn_id));
    txn.Commit();
}

std::tuple<std::string, std::string, std::string> G0Check(auto& db, int64_t account1_id, int64_t account2_id) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    auto p1_vid = vit.GetId();
    std::string p1_version_history = vit["versionHistory"].AsString();
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account2_id) break;
    }
    auto p2_vid = vit.GetId();
    std::string p2_version_history = vit["versionHistory"].AsString();
    auto TRANSFER = txn.GetEdgeLabelId("transfer");
    auto oeit = txn.GetOutEdgeIterator(lgraph_api::EdgeUid(p1_vid, p2_vid, TRANSFER, 0, 0));
    std::string k_version_history = oeit["versionHistory"].AsString();
    return std::make_tuple(p1_version_history, p2_version_history, k_version_history);
}

void G0Test(auto& db) {
    Initialize(db);

    G0Init(db);

    int64_t num_txns = 200;
    int64_t num_aborted_txns = 0;
#pragma omp parallel for reduction(+:num_aborted_txns)
    for (int64_t i = 1; i <= num_txns; i ++) {
        try {
            G0(db, 1, 2, i);
        } catch (std::exception& e) {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    std::string a1_version_history, a2_version_history, t_version_history;
    std::tie(a1_version_history, a2_version_history, t_version_history) = G0Check(db, 1, 2);

    std::cout << a1_version_history << std::endl;
    std::cout << a2_version_history << std::endl;
    std::cout << t_version_history << std::endl;

    if (a1_version_history == t_version_history && a2_version_history == t_version_history) {
        std::cout << "G0Test passed" << std::endl;
    } else {
        std::cout << "G0Test failed" << std::endl;
    }
}

// Aborted Reads

void G1AInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Account",
        {"id", "balance"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Double(99)
        }
    );
    txn.Commit();
}

void G1AW(auto& db, int64_t account1_id, int64_t sleep_ms) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    vit.SetField("balance", lgraph_api::FieldData::Double(200));
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    txn.Abort();
}

double G1AR(auto& db, int64_t account1_id) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    return vit["balance"].AsDouble();
}

void G1ATest(auto& db) {
    Initialize(db);

    G1AInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 5;
        int64_t num_aborted_txns = 0;
#pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                G1AW(db, 1, 250);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 5;
        int64_t num_incorrect_checks = 0;
#pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            auto a_balance = G1AR(db, 1);
            if (a_balance != 99) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "G1ATest passed" << std::endl;
    } else {
        std::cout << "G1ATest failed" << std::endl;
    }
}

// Intermediate Reads

void G1BInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Account",
        {"id", "balance"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Double(99)
        }
    );
    txn.Commit();
}

void G1BW(auto& db, int64_t account1_id, int64_t sleep_ms, int64_t even, int64_t odd) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    vit.SetField("balance", lgraph_api::FieldData::Double(even));
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    vit.SetField("balance", lgraph_api::FieldData::Double(odd));
    txn.Commit();
}

double G1BR(auto& db, int64_t account1_id) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    return vit["balance"].AsDouble();
}

void G1BTest(auto& db) {
    Initialize(db);

    G1BInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 50;
        int64_t num_aborted_txns = 0;
#pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                G1BW(db, 1, 250, 200, 99);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 100;
        int64_t num_incorrect_checks = 0;
#pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            auto a_balance = G1BR(db, 1);
            if (int64_t(a_balance) % 2 != 1) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "G1BTest passed" << std::endl;
    } else {
        std::cout << "G1BTest failed" << std::endl;
    }
}

// Circular Information Flow

void G1CInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Account",
        {"id", "balance"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Double(0)
        }
    );
    txn.AddVertex(
        "Account",
        {"id", "balance"},
        {
            lgraph_api::FieldData::Int64(2),
            lgraph_api::FieldData::Double(0)
        }
    );
    txn.Commit();
}

double G1C(auto& db, int64_t account1_id, int64_t account2_id, int64_t txn_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    vit.SetField("balance", lgraph_api::FieldData::Double(txn_id));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account2_id) break;
    }
    double p2_balance = vit["balance"].AsDouble();
    txn.Commit();
    return p2_balance;
}

void G1CTest(auto& db) {
    Initialize(db);

    G1CInit(db);

    int64_t c = 100;

    std::vector<int64_t> results(c);

    std::vector<std::mt19937> gens;
    for (int i = 0; i < omp_get_num_procs(); i ++) {
        gens.emplace_back(i);
    }

#pragma omp parallel for
    for (int64_t i = 1; i <= c; i ++) {
        auto& gen = gens[omp_get_thread_num()];
        std::uniform_int_distribution<> dist(0, 1);
        try {
            if (dist(gen)) {
                results[i - 1] = G1C(db, 1, 2, i);
            } else {
                results[i - 1] = G1C(db, 2, 1, i);
            }
        } catch (std::exception& e) {
            results[i - 1] = -1;
        }
    }

    int64_t num_aborted_txns = 0;
    int64_t num_incorrect_checks = 0;
    for (int64_t i = 1; i <= c; i ++) {
        auto v1 = results[i - 1];
        if (v1 == -1) {
            num_aborted_txns ++;
        }
        if (v1 == 0) continue;
        auto v2 = results[v1 - 1];
        if (v2 == -1 || i == v2) {
            num_incorrect_checks ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "G1CTest passed" << std::endl;
    } else {
        std::cout << "G1CTest failed" << std::endl;
    }
}

// Item-Many-Preceders

void IMPInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Account",
        {"id", "balance"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Double(1)
        }
    );
    txn.Commit();
}

void IMPW(auto& db, int64_t account1_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    vit.SetField("balance", lgraph_api::FieldData::Double(vit["balance"].AsDouble() + 1));
    txn.Commit();
}

std::tuple<double, double> IMPR(auto& db, int64_t account1_id, int64_t sleep_ms) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    double v1 = vit["balance"].AsDouble();
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    double v2 = vit["balance"].AsDouble();
    return std::make_tuple(v1, v2);
}

void IMPTest(auto& db) {
    Initialize(db);

    IMPInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 50;
        int64_t num_aborted_txns = 0;
#pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                IMPW(db, 1);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 50;
        int64_t num_incorrect_checks = 0;
#pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            int64_t v1, v2;
            std::tie(v1, v2) = IMPR(db, 1, 250);
            if (v1 != v2) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "IMPTest passed" << std::endl;
    } else {
        std::cout << "IMPTest failed" << std::endl;
    }
}

// Predicate-Many-Preceders

void PMPInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Account",
        {"id"},
        {
            lgraph_api::FieldData::Int64(1)
        }
    );
    txn.AddVertex(
        "Account",
        {"id"},
        {
            lgraph_api::FieldData::Int64(2)
        }
    );
    txn.Commit();
}

void PMPW(auto& db, int64_t account1_id, int64_t account2_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    auto a1_vid = vit.GetId();
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account2_id) break;
    }
    auto a2_vid = vit.GetId();
    txn.AddEdge(
        a1_vid, a2_vid, "transfer",
        std::vector<std::string>{},
        std::vector<lgraph_api::FieldData>{}
    );
    txn.Commit();
}

std::tuple<int64_t, int64_t> PMPR(auto& db, int64_t account_id, int64_t sleep_ms) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account_id) break;
    }
    int64_t c1 = 0;
    for (auto ieit = vit.GetInEdgeIterator(); ieit.IsValid(); ieit.Next()) {
        if (ieit.GetLabel() == "transfer") c1 ++;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    vit.Goto(0, true);
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account_id) break;
    }
    int64_t c2 = 0;
    for (auto ieit = vit.GetInEdgeIterator(); ieit.IsValid(); ieit.Next()) {
        if (ieit.GetLabel() == "transfer") c2 ++;
    }
    return std::make_tuple(c1, c2);
}

void PMPTest(auto& db) {
    Initialize(db);

    PMPInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 50;
        int64_t num_aborted_txns = 0;
#pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                PMPW(db, 1, 2);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 50;
        int64_t num_incorrect_checks = 0;
#pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            int64_t v1, v2;
            std::tie(v1, v2) = PMPR(db, 1, 250);
            if (v1 != v2) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "PMPTest passed" << std::endl;
    } else {
        std::cout << "PMPTest failed" << std::endl;
    }
}

// Observed Transaction Vanishes

void OTVInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    std::vector<int64_t> vids;
    for (int i = 1; i <= 4; i ++) {
        auto vid = txn.AddVertex(
            "Account",
            {"id", "balance"},
            {
                lgraph_api::FieldData::Int64(i),
                lgraph_api::FieldData::Double(0)
            }
        );
        vids.emplace_back(vid);
    }
    for (int i = 0; i < 4; i ++) {
        txn.AddEdge(
            vids[i], vids[(i + 1) % 4], "transfer",
            std::vector<std::string>{},
            std::vector<lgraph_api::FieldData>{}
        );
    }
    txn.Commit();
}

void OTVW(auto& db, int64_t account_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit1 = txn.GetVertexIterator();
    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Account" && vit1["id"].AsInt64() == account_id) break;
    }
    int64_t vid1 = vit1.GetId();
    for (auto eit1 = vit1.GetOutEdgeIterator(); eit1.IsValid(); eit1.Next()) {
        int64_t vid2 = eit1.GetDst();
        auto vit2 = txn.GetVertexIterator(vid2);
        for (auto eit2 = vit2.GetOutEdgeIterator(); eit2.IsValid(); eit2.Next()) {
            int64_t vid3 = eit2.GetDst();
            auto vit3 = txn.GetVertexIterator(vid3);
            for (auto eit3 = vit3.GetOutEdgeIterator(); eit3.IsValid(); eit3.Next()) {
                int64_t vid4 = eit3.GetDst();
                auto vit4 = txn.GetVertexIterator(vid4);
                for (auto eit4 = vit4.GetOutEdgeIterator(); eit4.IsValid(); eit4.Next()) {
                    if (eit4.GetDst() == vid1) {
                        vit1.SetField("balance", lgraph_api::FieldData::Double(vit1["balance"].AsDouble() + 1));
                        vit2.SetField("balance", lgraph_api::FieldData::Double(vit2["balance"].AsDouble() + 1));
                        vit3.SetField("balance", lgraph_api::FieldData::Double(vit3["balance"].AsDouble() + 1));
                        vit4.SetField("balance", lgraph_api::FieldData::Double(vit4["balance"].AsDouble() + 1));
                        txn.Commit();
                        return;
                    }
                }
            }
        }
    }
}

std::tuple< std::tuple<double, double, double, double>, std::tuple<double, double, double, double> > OTVR(auto& db, int64_t account_id, int64_t sleep_ms) {
    auto txn = db.CreateReadTxn();
    auto vit1 = txn.GetVertexIterator();

    auto get_versions = [&]() -> std::tuple<int64_t, int64_t, int64_t, int64_t> {
        int64_t vid1 = vit1.GetId();
        for (auto eit1 = vit1.GetOutEdgeIterator(); eit1.IsValid(); eit1.Next()) {
            int64_t vid2 = eit1.GetDst();
            auto vit2 = txn.GetVertexIterator(vid2);
            for (auto eit2 = vit2.GetOutEdgeIterator(); eit2.IsValid(); eit2.Next()) {
                int64_t vid3 = eit2.GetDst();
                auto vit3 = txn.GetVertexIterator(vid3);
                for (auto eit3 = vit3.GetOutEdgeIterator(); eit3.IsValid(); eit3.Next()) {
                    int64_t vid4 = eit3.GetDst();
                    auto vit4 = txn.GetVertexIterator(vid4);
                    for (auto eit4 = vit4.GetOutEdgeIterator(); eit4.IsValid(); eit4.Next()) {
                        if (eit4.GetDst() == vid1) {
                            return std::make_tuple(
                                vit1["balance"].AsDouble(), vit2["balance"].AsDouble(), vit3["balance"].AsDouble(), vit4["balance"].AsDouble()
                            );
                        }
                    }
                }
            }
        }
    };

    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Account" && vit1["id"].AsInt64() == account_id) break;
    }
    auto tup1 = get_versions();

    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

    vit1.Goto(0, true);
    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Account" && vit1["id"].AsInt64() == account_id) break;
    }
    auto tup2 = get_versions();

    return std::make_tuple(tup1, tup2);
}

void OTVTest(auto& db) {
    Initialize(db);

    OTVInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 50;
        int64_t num_aborted_txns = 0;
#pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                OTVW(db, 1);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 50;
        int64_t num_incorrect_checks = 0;
#pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
            std::tie(tup1, tup2) = OTVR(db, 1, 250);
            int64_t v1_max = std::max(std::max(std::get<0>(tup1), std::get<1>(tup1)), std::max(std::get<2>(tup1), std::get<3>(tup1)));
            int64_t v2_min = std::min(std::min(std::get<0>(tup2), std::get<1>(tup2)), std::min(std::get<2>(tup2), std::get<3>(tup2)));
            if (v1_max > v2_min) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "OTVTest passed" << std::endl;
    } else {
        std::cout << "OTVTest failed" << std::endl;
    }
}

// Fractured Reads

void FRInit(auto& db) {
    OTVInit(db);
}

void FRW(auto& db, int64_t account_id) {
    OTVW(db, account_id);
}

std::tuple< std::tuple<int64_t, int64_t, int64_t, int64_t>, std::tuple<int64_t, int64_t, int64_t, int64_t> > FRR(auto& db, int64_t account_id, int64_t sleep_ms) {
    return OTVR(db, account_id, sleep_ms);
}

void FRTest(auto& db) {
    Initialize(db);

    FRInit(db);

    std::promise<int64_t> p1;
    auto f1 = p1.get_future();
    std::thread t1([&](std::promise<int64_t> && p1){
        int wc = 100;
        int64_t num_aborted_txns = 0;
#pragma omp parallel for reduction(+:num_aborted_txns)
        for (int i = 0; i < wc; i ++) {
            try {
                FRW(db, 1);
            } catch (std::exception& e) {
                num_aborted_txns ++;
            }
        }
        p1.set_value(num_aborted_txns);
    }, std::move(p1));

    std::promise<int64_t> p2;
    auto f2 = p2.get_future();
    std::thread t2([&](std::promise<int64_t> && p2){
        int rc = 100;
        int64_t num_incorrect_checks = 0;
#pragma omp parallel for reduction(+:num_incorrect_checks)
        for (int i = 0; i < rc; i ++) {
            std::tuple<int64_t, int64_t, int64_t, int64_t> tup1, tup2;
            std::tie(tup1, tup2) = FRR(db, 1, 250);
            if (tup1 != tup2) num_incorrect_checks ++;
        }
        p2.set_value(num_incorrect_checks);
    }, std::move(p2));

    t1.join();
    t2.join();
    int64_t num_aborted_txns = f1.get();
    int64_t num_incorrect_checks = f2.get();

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    if (num_incorrect_checks == 0) {
        std::cout << "FRTest passed" << std::endl;
    } else {
        std::cout << "FRTest failed" << std::endl;
    }
}

// Lost Updates

void LUInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    txn.AddVertex(
        "Account",
        {"id", "numTransferred"},
        {
            lgraph_api::FieldData::Int64(1),
            lgraph_api::FieldData::Int64(0)
        }
    );
    txn.Commit();
}

void LUW(auto& db, int64_t account1_id, int64_t account2_id) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account1_id) break;
    }
    vit.SetField("numTransferred", lgraph_api::FieldData::Int64(vit["numTransferred"].AsInt64() + 1));
    auto p1_vid = vit.GetId();
    auto p2_vid = txn.AddVertex(
        "Account",
        {"id"},
        {lgraph_api::FieldData::Int64(account2_id)}
    );
    txn.AddEdge(
        p1_vid, p2_vid, "transfer",
        std::vector<std::string>{},
        std::vector<lgraph_api::FieldData>{}
    );
    txn.Commit();
}

std::tuple<int64_t, int64_t> LUR(auto& db, int64_t account_id) {
    auto txn = db.CreateReadTxn();
    auto vit = txn.GetVertexIterator();
    for (; vit.IsValid(); vit.Next()) {
        if (vit.GetLabel() == "Account" && vit["id"].AsInt64() == account_id) break;
    }
    int64_t num_transfer_prop = vit["numTransferred"].AsInt64();
    int64_t num_transfer_edges = 0;
    for (auto oeit = vit.GetOutEdgeIterator(); oeit.IsValid(); oeit.Next()) {
        if (oeit.GetLabel() == "transfer") num_transfer_edges ++;
    }
    return std::make_tuple(num_transfer_prop, num_transfer_edges);
}

void LUTest(auto& db) {
    Initialize(db);

    LUInit(db);

    int64_t num_txns = 200;
    int64_t num_aborted_txns = 0;
#pragma omp parallel for reduction(+:num_aborted_txns)
    for (int64_t i = 0; i < num_txns; i ++) {
        try {
            LUW(db, 1, i + 2);
        } catch (std::exception& e) {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    int64_t num_transfer_prop, num_transfer_edges;
    std::tie(num_transfer_prop, num_transfer_edges) = LUR(db, 1);

    std::cout << num_txns << " " << num_aborted_txns << " " << num_transfer_prop << " " << num_transfer_edges << std::endl;

    if (num_transfer_prop == num_txns - num_aborted_txns && num_transfer_edges == num_txns - num_aborted_txns) {
        std::cout << "LUTest passed" << std::endl;
    } else {
        std::cout << "LUTest failed" << std::endl;
    }
}

// Write Skews

void WSInit(auto& db) {
    auto txn = db.CreateWriteTxn(optimistic);
    for (int i = 1; i <= 10; i ++) {
        txn.AddVertex(
            "Account",
            {"id", "balance"},
            {
                lgraph_api::FieldData::Int64(2 * i - 1),
                lgraph_api::FieldData::Double(70)
            }
        );
        txn.AddVertex(
            "Account",
            {"id", "balance"},
            {
                lgraph_api::FieldData::Int64(2 * i),
                lgraph_api::FieldData::Double(80)
            }
        );
    }
    txn.Commit();
}

void WSW(auto& db, int64_t account1_id, int64_t account2_id, int64_t sleep_ms, std::mt19937& gen) {
    auto txn = db.CreateWriteTxn(optimistic);
    auto vit1 = txn.GetVertexIterator();
    for (; vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() == "Account" && vit1["id"].AsInt64() == account1_id) break;
    }
    int64_t vid1 = vit1.GetId();
    double p1_value = vit1["balance"].AsDouble();
    auto vit2 = txn.GetVertexIterator();
    for (; vit2.IsValid(); vit2.Next()) {
        if (vit2.GetLabel() == "Account" && vit2["id"].AsInt64() == account2_id) break;
    }
    int64_t vid2 = vit2.GetId();
    double p2_value = vit2["balance"].AsDouble();
    if (p1_value + p2_value - 100 < 0) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    std::uniform_int_distribution<> dist(0, 1);
    if (dist(gen)) {
        vit1.SetField("balance", lgraph_api::FieldData::Double(p1_value - 100));
    } else {
        vit2.SetField("balance", lgraph_api::FieldData::Double(p2_value - 100));
    }
    txn.Commit();
}

std::vector< std::tuple<int64_t, double, int64_t, double> > WSR(auto& db) {
    std::vector< std::tuple<int64_t, double, int64_t, double> > results;
    auto txn = db.CreateReadTxn();
    for (auto vit1 = txn.GetVertexIterator(); vit1.IsValid(); vit1.Next()) {
        if (vit1.GetLabel() != "Account") continue;
        int64_t account1_id = vit1["id"].AsInt64();
        if (account1_id % 2 != 1) continue;
        double p1_value = vit1["balance"].AsDouble();
        for (auto vit2 = txn.GetVertexIterator(); vit2.IsValid(); vit2.Next()) {
            if (vit2.GetLabel() != "Account") continue;
            int64_t account2_id = vit2["id"].AsInt64();
            if (account2_id != account1_id + 1) continue;
            double p2_value = vit2["balance"].AsDouble();
            if (p1_value + p2_value <= 0) {
                results.emplace_back(account1_id, p1_value, account2_id, p2_value);
            }
        }
    }
    return results;
}

void WSTest(auto& db) {
    Initialize(db);

    WSInit(db);

    int wc = 50;

    int64_t num_aborted_txns = 0;

    std::vector<std::mt19937> gens;
    for (int i = 0; i < omp_get_num_procs(); i ++) {
        gens.emplace_back(i);
    }

#pragma omp parallel for reduction(+:num_aborted_txns)
    for (int i = 0; i < wc; i ++) {
        auto& gen = gens[omp_get_thread_num()];
        std::uniform_int_distribution<> dist(1, 10);
        try {
            int64_t account1_id = dist(gen) * 2 - 1;
            int64_t account2_id = account1_id + 1;
            WSW(db, account1_id, account2_id, 250, gen);
        } catch (std::exception& e) {
            num_aborted_txns ++;
        }
    }

    std::cout << "Number of aborted txns: " << num_aborted_txns << std::endl;

    auto results = WSR(db);

    if (results.empty()) {
        std::cout << "WSTest passed" << std::endl;
    } else {
        std::cout << "WSTest failed" << std::endl;
        for (auto& tup : results) {
            std::cout << std::get<0>(tup) << " " << std::get<1>(tup) << " " << std::get<2>(tup) << " " << std::get<3>(tup) << std::endl;
        }
    }
}

void TestAll(auto& db) {
    AtomicityCTest(db);

    AtomicityRBTest(db);

    G0Test(db);

    G1ATest(db);

    G1BTest(db);

    G1CTest(db);

    IMPTest(db);

    PMPTest(db);

    OTVTest(db);

    FRTest(db);

    LUTest(db);

    WSTest(db);
}

int main(int argc, char ** argv) {
    std::string db_path = "./testdb";

    lgraph_api::Galaxy galaxy(db_path, "admin", "73@TuGraph", false, true);
    auto db = galaxy.OpenGraph("default");

    optimistic = false;
    std::cout << "Serializable" << std::endl;
    std::cout << "--------------------" << std::endl;
    TestAll(db);
    std::cout << "--------------------" << std::endl;

    return 0;
}