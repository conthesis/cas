import orjson

from cas.normalizer import Normalizer

nm = Normalizer()


def test_normalize_stable():
    def json2json(x):
        assert orjson.loads(nm.normalize(orjson.dumps(x))) == x

    json2json({})
    json2json({"a": {"b": {"c": ["d", ["e", "f", {"h": "i"}]]}}})
    json2json([])


def test_normalize_sort():
    # XXX: sensitive test but we need to test for it.
    assert nm.normalize(orjson.dumps({"c": 3, "b": 2, "a": 1})) == b'{"a":1,"b":2,"c":3}'


def test_identify():
    res = nm.identify(b"hello world")
    assert len(res) == 8
    res = nm.identify(b"hello world" * 8)
    assert len(res) == 8
