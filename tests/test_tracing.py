"""Tests for the W3C Trace Context module."""

from moonraker_owl.tracing import TraceContext, create_traceparent


class TestTraceContext:
    """Tests for TraceContext class."""

    def test_create_generates_valid_traceparent(self) -> None:
        """TraceContext.create() generates valid W3C traceparent format."""
        ctx = TraceContext.create()

        traceparent = ctx.traceparent
        parts = traceparent.split("-")

        assert len(parts) == 4
        assert parts[0] == "00"  # version
        assert len(parts[1]) == 32  # trace-id
        assert len(parts[2]) == 16  # span-id
        assert parts[3] == "01"  # sampled flag

    def test_create_unsampled(self) -> None:
        """TraceContext.create(sampled=False) sets correct flag."""
        ctx = TraceContext.create(sampled=False)

        parts = ctx.traceparent.split("-")
        assert parts[3] == "00"  # not sampled

    def test_trace_id_is_unique(self) -> None:
        """Each call to create() generates unique trace IDs."""
        ctx1 = TraceContext.create()
        ctx2 = TraceContext.create()

        assert ctx1.trace_id != ctx2.trace_id
        assert ctx1.span_id != ctx2.span_id

    def test_parse_valid_traceparent(self) -> None:
        """TraceContext.parse() correctly parses valid traceparent."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        ctx = TraceContext.parse(traceparent)

        assert ctx is not None
        assert ctx.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert ctx.span_id == "00f067aa0ba902b7"
        assert ctx.sampled is True

    def test_parse_unsampled(self) -> None:
        """TraceContext.parse() correctly parses unsampled traceparent."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
        ctx = TraceContext.parse(traceparent)

        assert ctx is not None
        assert ctx.sampled is False

    def test_parse_invalid_returns_none(self) -> None:
        """TraceContext.parse() returns None for invalid input."""
        assert TraceContext.parse("") is None
        assert TraceContext.parse("invalid") is None
        assert TraceContext.parse("00-short-short-01") is None
        # All-zero trace-id is invalid
        assert TraceContext.parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01") is None
        # All-zero span-id is invalid
        assert TraceContext.parse("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01") is None

    def test_parse_roundtrip(self) -> None:
        """Parsing a created traceparent produces identical values."""
        original = TraceContext.create()
        parsed = TraceContext.parse(original.traceparent)

        assert parsed is not None
        assert parsed.trace_id == original.trace_id
        assert parsed.span_id == original.span_id
        assert parsed.sampled == original.sampled


class TestCreateTraceparent:
    """Tests for create_traceparent convenience function."""

    def test_returns_valid_format(self) -> None:
        """create_traceparent() returns valid W3C format."""
        traceparent = create_traceparent()

        parts = traceparent.split("-")
        assert len(parts) == 4
        assert parts[0] == "00"
        assert len(parts[1]) == 32
        assert len(parts[2]) == 16
        assert parts[3] == "01"

    def test_unsampled_flag(self) -> None:
        """create_traceparent(sampled=False) sets correct flag."""
        traceparent = create_traceparent(sampled=False)
        assert traceparent.endswith("-00")
