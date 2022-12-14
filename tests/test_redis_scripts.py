import pytest
import redis

from .utils import get_tiger


class TestRedisScripts:
    def setup_method(self, method):
        self.tiger = get_tiger()
        self.conn = self.tiger.connection
        self.conn.flushdb()
        self.scripts = self.tiger.scripts

    def teardown_method(self, method):
        self.conn.flushdb()
        self.conn.close()
        # Force disconnect so we don't get Too many open files
        self.conn.connection_pool.disconnect()

    def _test_zadd(self, mode):
        self.conn.zadd("z", {"key1": 2})
        self.scripts.zadd("z", 4, "key1", mode=mode)
        self.scripts.zadd("z", 3, "key1", mode=mode)
        self.scripts.zadd("z", 1, "key2", mode=mode)
        self.scripts.zadd("z", 2, "key2", mode=mode)
        self.scripts.zadd("z", 0, "key2", mode=mode)
        return self.conn.zrange("z", 0, -1, withscores=True)

    def test_zadd_nx(self):
        entries = self._test_zadd("nx")
        assert entries == [("key2", 1.0), ("key1", 2.0)]

    def test_zadd_xx(self):
        entries = self._test_zadd("xx")
        assert entries == [("key1", 3.0)]

    def test_zadd_min(self):
        entries = self._test_zadd("min")
        assert entries == [("key2", 0.0), ("key1", 2.0)]

    def test_zadd_max(self):
        entries = self._test_zadd("max")
        assert entries == [("key2", 2.0), ("key1", 4.0)]

    def test_zpoppush_1(self):
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        result = self.scripts.zpoppush("src", "dst", 3, None, 10)
        assert result == ["a", "b", "c"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == [("d", 4.0)]

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 10.0), ("b", 10.0), ("c", 10.0)]

    def test_zpoppush_2(self):
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        result = self.scripts.zpoppush("src", "dst", 100, None, 10)
        assert result == ["a", "b", "c", "d"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == []

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 10.0), ("b", 10.0), ("c", 10.0), ("d", 10.0)]

    def test_zpoppush_3(self):
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        result = self.scripts.zpoppush("src", "dst", 3, 2, 10)
        assert result == ["a", "b"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == [("c", 3.0), ("d", 4.0)]

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 10.0), ("b", 10.0)]

    def test_zpoppush_withscores_1(self):
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        result = self.scripts.zpoppush(
            "src", "dst", 3, None, 10, withscores=True
        )
        assert result == ["a", "1", "b", "2", "c", "3"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == [("d", 4.0)]

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 10.0), ("b", 10.0), ("c", 10.0)]

    def test_zpoppush_withscores_2(self):
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        result = self.scripts.zpoppush(
            "src", "dst", 100, None, 10, withscores=True
        )
        assert result == ["a", "1", "b", "2", "c", "3", "d", "4"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == []

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 10.0), ("b", 10.0), ("c", 10.0), ("d", 10.0)]

    def test_zpoppush_withscores_3(self):
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        result = self.scripts.zpoppush("src", "dst", 3, 2, 10, withscores=True)
        assert result == ["a", "1", "b", "2"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == [("c", 3.0), ("d", 4.0)]

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 10.0), ("b", 10.0)]

    def test_zpoppush_on_success_1(self, **kwargs):
        """
        2 out of 4 items moved, so add_set contains "val"
        """
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        # Whether the members are in the destination ZSET doesn't make any
        # difference here.
        self.conn.zadd("dst", {"a": 5, "b": 5})
        self.conn.sadd("remove_set", "val")
        result = self.scripts.zpoppush(
            "src",
            "dst",
            count=2,
            score=None,
            new_score=10,
            on_success=("update_sets", "val", "remove_set", "add_set"),
            **kwargs
        )
        assert result == ["a", "b"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == [("c", 3.0), ("d", 4.0)]

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 10.0), ("b", 10.0)]

        assert self.conn.smembers("remove_set") == {"val"}
        assert self.conn.smembers("add_set") == {"val"}

    def test_zpoppush_on_success_2(self, **kwargs):
        """
        0 out of 4 items moved, so no sets were changed
        """
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        self.conn.sadd("remove_set", "val")
        result = self.scripts.zpoppush(
            "src",
            "dst",
            count=2,
            score=0,
            new_score=10,
            on_success=("update_sets", "val", "remove_set", "add_set"),
            **kwargs
        )
        assert result == []

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)]

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == []

        assert self.conn.smembers("remove_set") == {"val"}
        assert self.conn.smembers("add_set") == set()

    def test_zpoppush_on_success_3(self, **kwargs):
        """
        4 out of 4 items moved, so both sets were changed
        """
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        self.conn.sadd("remove_set", "val")
        result = self.scripts.zpoppush(
            "src",
            "dst",
            count=4,
            score=None,
            new_score=10,
            on_success=("update_sets", "val", "remove_set", "add_set"),
            **kwargs
        )
        assert result == ["a", "b", "c", "d"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == []

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 10), ("b", 10), ("c", 10), ("d", 10)]

        assert self.conn.smembers("remove_set") == set()
        assert self.conn.smembers("add_set") == {"val"}

    def test_zpoppush_ignore_if_exists_1(self):
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        # Members that are in the destination ZSET are not updated here.
        self.conn.zadd("dst", {"a": 5, "b": 5})
        self.conn.sadd("remove_set", "val")
        result = self.scripts.zpoppush(
            "src",
            "dst",
            count=2,
            score=None,
            new_score=10,
            on_success=("update_sets", "val", "remove_set", "add_set"),
            if_exists=("noupdate",),
        )
        assert result == []

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == [("c", 3.0), ("d", 4.0)]

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 5.0), ("b", 5.0)]

        assert self.conn.smembers("remove_set") == {"val"}
        assert self.conn.smembers("add_set") == {"val"}

    def test_zpoppush_ignore_if_exists_2(self):
        self.test_zpoppush_on_success_2(if_exists=("noupdate",))

    def test_zpoppush_ignore_if_exists_3(self):
        self.test_zpoppush_on_success_3(if_exists=("noupdate",))

    def _test_zpoppush_min_if_exists(self, expected_if_exists_score):
        self.conn.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        # Members that are in the destination ZSET are added to the if_exists
        # ZSET.
        self.conn.zadd("dst", {"a": 5})
        self.conn.sadd("remove_set", "val")
        result = self.scripts.zpoppush(
            "src",
            "dst",
            count=2,
            score=None,
            new_score=10,
            on_success=(
                "update_sets",
                "val",
                "remove_set",
                "add_set",
                "add_set_if_exists",
            ),
            if_exists=("add", "if_exists", 20, "min"),
        )
        assert result == ["b"]

        src = self.conn.zrange("src", 0, -1, withscores=True)
        assert src == [("c", 3.0), ("d", 4.0)]

        dst = self.conn.zrange("dst", 0, -1, withscores=True)
        assert dst == [("a", 5.0), ("b", 10.0)]

        if_exists = self.conn.zrange("if_exists", 0, -1, withscores=True)
        assert if_exists == [("a", expected_if_exists_score)]

        assert self.conn.smembers("remove_set") == {"val"}
        assert self.conn.smembers("add_set") == {"val"}
        assert self.conn.smembers("add_set_if_exists") == {"val"}

    def test_zpoppush_min_if_exists_1(self):
        self._test_zpoppush_min_if_exists(20)

    def test_zpoppush_min_if_exists_2(self):
        self.conn.zadd("if_exists", {"a": 10})
        self._test_zpoppush_min_if_exists(10)

    def test_zpoppush_min_if_exists_3(self):
        self.conn.zadd("if_exists", {"a": 30})
        self._test_zpoppush_min_if_exists(20)

    def test_srem_if_not_exists_1(self):
        self.conn.sadd("set", "member")
        result = self.scripts.srem_if_not_exists("set", "member", "other_key")
        assert result == 1
        assert self.conn.smembers("set") == set()

    def test_srem_if_not_exists_2(self):
        self.conn.sadd("set", "member")
        self.conn.set("other_key", 0)
        result = self.scripts.srem_if_not_exists("set", "member", "other_key")
        assert result == 0
        assert self.conn.smembers("set") == {"member"}

    def test_delete_if_not_in_zsets_1(self):
        self.conn.set("foo", 0)
        self.conn.set("bar", 0)
        self.conn.set("baz", 0)

        self.conn.zadd("z2", {"other": 0})
        result = self.scripts.delete_if_not_in_zsets(
            to_delete=["foo", "bar"], value="member", zsets=["z1", "z2"]
        )
        assert result == 2
        assert self.conn.exists("foo") == 0
        assert self.conn.exists("bar") == 0
        assert self.conn.exists("baz") == 1
        assert self.conn.exists("z2") == 1

    def test_delete_if_not_in_zsets_2(self):
        self.conn.set("foo", 0)
        self.conn.set("bar", 0)

        self.conn.zadd("z2", {"member": 0})
        result = self.scripts.delete_if_not_in_zsets(
            to_delete=["foo", "bar"], value="member", zsets=["z1", "z2"]
        )
        assert result == 0
        assert self.conn.exists("foo") == 1
        assert self.conn.exists("bar") == 1
        assert self.conn.exists("z2") == 1

    def test_delete_if_not_in_zsets_3(self):
        self.conn.set("foo", 0)
        self.conn.set("bar", 0)

        result = self.scripts.delete_if_not_in_zsets(
            to_delete=["foo", "bar"], value="member", zsets=[]
        )
        assert result == 2
        assert self.conn.exists("foo") == 0
        assert self.conn.exists("bar") == 0

    def test_get_expired_tasks(self):
        self.conn.sadd("t:active", "q1", "q2", "q3", "q4")
        self.conn.zadd("t:active:q1", {"t1": 500})
        self.conn.zadd("t:active:q1", {"t2": 1000})
        self.conn.zadd("t:active:q1", {"t3": 1500})
        self.conn.zadd("t:active:q2", {"t4": 1200})
        self.conn.zadd("t:active:q3", {"t5": 1800})
        self.conn.zadd("t:active:q4", {"t6": 200})

        expired_task_set = {("q1", "t1"), ("q1", "t2"), ("q4", "t6")}

        result = self.scripts.get_expired_tasks("t", 1000, 10)
        assert len(result) == 3
        assert set(result) == expired_task_set

        for batch_size in range(1, 4):
            result = self.scripts.get_expired_tasks("t", 1000, batch_size)
            assert len(result) == batch_size
            assert set(result) & expired_task_set == set(result)

    def test_execute_pipeline_1(self):
        p = self.conn.pipeline()
        # Test basic commands
        p.lpush("x", "a")
        p.lpush("x", "b")
        p.lrange("x", 0, -1)
        p.zadd("src", {"a": 1, "b": 2, "c": 3, "d": 4})
        # Ensure custom scripts work (ZPOPPUSH has both KEYS and ARGS)
        self.scripts.zpoppush("src", "dst", 3, None, 10, client=p)
        # Ensure raw responses ('OK') are converted into Python values (True)
        p.set("y", 1)
        results = self.scripts.execute_pipeline(p)
        assert results == [1, 2, ["b", "a"], 4, ["a", "b", "c"], True]

    def test_execute_pipeline_2(self):
        p = self.conn.pipeline()
        p.set("x", 1)
        # Test that invalid operation halts pipeline.
        p.lrange("x", 0, -1)
        p.set("y", 1)
        pytest.raises(redis.ResponseError, self.scripts.execute_pipeline, p)
        assert self.conn.get("x") == "1"
        assert self.conn.get("y") is None

    @pytest.mark.parametrize("can_replicate_commands", [True, False])
    def test_execute_pipeline_script(self, can_replicate_commands):
        if not self.scripts.can_replicate_commands:
            assert False, "test suite needs Redis 3.2 or higher"

        self.scripts._can_replicate_commands = can_replicate_commands

        self.conn.script_flush()

        s = self.conn.register_script("redis.call('set', 'x', 'y')")

        # Uncached execution
        p = self.conn.pipeline()
        s(client=p)
        self.scripts.execute_pipeline(p)
        assert self.conn.get("x") == "y"

        self.conn.delete("x")

        # Cached execution
        p = self.conn.pipeline()
        s(client=p)
        self.scripts.execute_pipeline(p)
        assert self.conn.get("x") == "y"
