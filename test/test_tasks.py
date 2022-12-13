from katsdpcontroller.tasks import KatcpTransition


class TestKatcpTransition:
    def test_format(self):
        orig = KatcpTransition("{noformat}", "pos {}", "named {named}", 5, timeout=10)
        new = orig.format(123, named="foo")
        assert new.name == "{noformat}"
        assert new.args == ("pos 123", "named foo", 5)
        assert new.timeout == 10

    def test_repr(self):
        assert repr(KatcpTransition("name", timeout=5)) == "KatcpTransition('name', timeout=5)"
        assert (
            repr(KatcpTransition("name", "arg1", True, timeout=10))
            == "KatcpTransition('name', 'arg1', True, timeout=10)"
        )
