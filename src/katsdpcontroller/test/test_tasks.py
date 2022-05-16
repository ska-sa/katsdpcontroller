from nose.tools import assert_equal

from katsdpcontroller.tasks import KatcpTransition


class TestKatcpTransition:
    def test_format(self):
        orig = KatcpTransition('{noformat}', 'pos {}', 'named {named}', 5, timeout=10)
        new = orig.format(123, named='foo')
        assert_equal(new.name, '{noformat}')
        assert_equal(new.args, ('pos 123', 'named foo', 5))
        assert_equal(new.timeout, 10)

    def test_repr(self):
        assert_equal(repr(KatcpTransition('name', timeout=5)),
                     "KatcpTransition('name', timeout=5)")
        assert_equal(repr(KatcpTransition('name', 'arg1', True, timeout=10)),
                     "KatcpTransition('name', 'arg1', True, timeout=10)")
