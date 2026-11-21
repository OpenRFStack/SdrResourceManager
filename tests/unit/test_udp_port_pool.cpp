/*
========================================================================
Project: OpenRFStack
Author:  Brendan Michaud
Year:    2026
Part of OpenRFStack (https://github.com/OpenRFStack)

Licensed under the Personal Use License.
Do not use for commercial, organizational, or military purposes.
Contact author for permission: https://github.com/OpenRFStack
========================================================================
*/
#include "UdpPortPool.hpp"

#include <gtest/gtest.h>

#include <set>
#include <stdexcept>

using namespace sdr;

TEST(UdpPortPool, SingleAllocateAndRelease)
{
    UdpPortPool pool(30'000, 30'010);
    EXPECT_EQ(pool.freeCount(), 11);
    EXPECT_EQ(pool.usedCount(), 0);

    int p = pool.allocate();
    EXPECT_EQ(p, 30'000);
    EXPECT_EQ(pool.freeCount(), 10);
    EXPECT_EQ(pool.usedCount(), 1);

    pool.release(p);
    EXPECT_EQ(pool.freeCount(), 11);
    EXPECT_EQ(pool.usedCount(), 0);
}

TEST(UdpPortPool, AllocateNReturnsUniquePorts)
{
    UdpPortPool pool(40'000, 40'099);
    auto        ports = pool.allocateN(4);
    ASSERT_EQ(ports.size(), 4u);

    std::set<int> ps(ports.begin(), ports.end());
    EXPECT_EQ(ps.size(), 4u);
    for (int p : ports)
    {
        EXPECT_GE(p, 40'000);
        EXPECT_LE(p, 40'099);
    }
    EXPECT_EQ(pool.usedCount(), 4);
    EXPECT_EQ(pool.freeCount(), 96);
}

TEST(UdpPortPool, AllocateNFailsWhenExhausted)
{
    UdpPortPool pool(50'000, 50'002);  // 3 ports total
    auto        ok = pool.allocateN(3);
    ASSERT_EQ(ok.size(), 3u);

    auto fail = pool.allocateN(1);
    EXPECT_TRUE(fail.empty());

    EXPECT_EQ(pool.allocate(), -1);
}

TEST(UdpPortPool, ReleaseAllReturnsAllPorts)
{
    UdpPortPool pool(60'000, 60'009);
    auto        ports = pool.allocateN(5);
    ASSERT_EQ(ports.size(), 5u);

    pool.releaseAll(ports);
    EXPECT_EQ(pool.freeCount(), 10);
    EXPECT_EQ(pool.usedCount(), 0);
}

TEST(UdpPortPool, ReleaseOfUnallocatedPortIsIgnored)
{
    UdpPortPool pool(70'000, 70'005);
    pool.release(99'999);
    EXPECT_EQ(pool.freeCount(), 6);
    EXPECT_EQ(pool.usedCount(), 0);
}

TEST(UdpPortPool, ConstructorThrowsIfFirstGeqLast)
{
    EXPECT_THROW(UdpPortPool(5000, 4999), std::invalid_argument);
    EXPECT_THROW(UdpPortPool(5000, 5000), std::invalid_argument);
}

// ── Edge cases ────────────────────────────────────────────────────────────────

TEST(UdpPortPool, AllocateZeroReturnsEmpty)
{
    UdpPortPool pool(30'000, 30'010);
    auto        ports = pool.allocateN(0);
    EXPECT_TRUE(ports.empty());
    EXPECT_EQ(pool.usedCount(), 0);
    EXPECT_EQ(pool.freeCount(), 11);
}

TEST(UdpPortPool, DoubleReleaseIsNoOp)
{
    UdpPortPool pool(30'000, 30'010);
    int         p = pool.allocate();
    ASSERT_NE(p, -1);
    pool.release(p);
    EXPECT_EQ(pool.freeCount(), 11);
    // Releasing the same port a second time must not inflate freeCount.
    pool.release(p);
    EXPECT_EQ(pool.freeCount(), 11);
    EXPECT_EQ(pool.usedCount(), 0);
}

TEST(UdpPortPool, ExhaustThenReleaseAndReallocate)
{
    UdpPortPool pool(50'000, 50'002);  // 3 ports: 50000, 50001, 50002
    auto        ports = pool.allocateN(3);
    ASSERT_EQ(ports.size(), 3u);
    EXPECT_EQ(pool.freeCount(), 0);

    pool.release(ports[1]);
    EXPECT_EQ(pool.freeCount(), 1);
    EXPECT_EQ(pool.usedCount(), 2);

    int p = pool.allocate();
    EXPECT_NE(p, -1);
    EXPECT_EQ(pool.freeCount(), 0);
    EXPECT_EQ(pool.usedCount(), 3);
}

TEST(UdpPortPool, MinimalTwoPortPool)
{
    // Smallest valid pool: last = first + 1 → 2 ports.
    UdpPortPool pool(9999, 10'000);
    EXPECT_EQ(pool.freeCount(), 2);

    int p1 = pool.allocate();
    int p2 = pool.allocate();
    EXPECT_NE(p1, -1);
    EXPECT_NE(p2, -1);
    EXPECT_NE(p1, p2);
    EXPECT_GE(p1, 9999);
    EXPECT_LE(p1, 10'000);
    EXPECT_GE(p2, 9999);
    EXPECT_LE(p2, 10'000);
    EXPECT_EQ(pool.freeCount(), 0);
    EXPECT_EQ(pool.allocate(), -1);
}

TEST(UdpPortPool, AllocateNSkipsAvoidedPorts)
{
    sdr::UdpPortPool pool(100, 103);
    auto             ports = pool.allocateN(2, {100, 102});
    EXPECT_EQ(ports, (std::vector<int>{101, 103}));
    EXPECT_TRUE(pool.allocateN(1, {100, 102}).empty());  // only avoided ports left
    EXPECT_EQ(pool.freeCount(), 2);
}

/*
========================================================================
End of file — OpenRFStack
Subject to Personal Use License
https://github.com/OpenRFStack
========================================================================
*/
