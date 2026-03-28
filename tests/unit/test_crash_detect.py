"""Unit tests for server/crash_detect.py — crash and soft-failure detection."""

import pytest

from server.crash_detect import detect_crash, detect_soft_failure, is_benign_line


class TestDetectCrash:
    @pytest.mark.unit
    def test_traceback(self):
        content = "some output\nTraceback (most recent call last):\n  File ..."
        assert detect_crash(content) is not None

    @pytest.mark.unit
    def test_value_error(self):
        content = "ValueError: No files found with the given pattern."
        assert detect_crash(content) is not None

    @pytest.mark.unit
    def test_cuda_oom(self):
        content = "RuntimeError: CUDA out of memory"
        assert detect_crash(content) is not None

    @pytest.mark.unit
    def test_srun_error(self):
        assert detect_crash("srun: error: node42: task 0: Killed") is not None

    @pytest.mark.unit
    def test_clean_log(self):
        content = "Epoch 1/10: 100%|██████████| 1000/1000\nTraining complete."
        assert detect_crash(content) is None

    @pytest.mark.unit
    def test_empty_and_none(self):
        assert detect_crash("") is None
        assert detect_crash(None) is None

    @pytest.mark.unit
    def test_false_positive_filtered(self):
        content = "Sandbox state restoration failed — retrying\nTraining complete."
        assert detect_crash(content) is None


class TestDetectSoftFailure:
    @pytest.mark.unit
    def test_no_data_to_process(self):
        content = (
            "Waiting for the server to start...\n"
            "No data to process, exiting.\n"
            "ValueError: No files found\n"
        )
        result = detect_soft_failure(content)
        assert result is not None
        assert "No data to process" in result

    @pytest.mark.unit
    def test_exists_skipping(self):
        content = "File `/data/output.jsonl` exists, skipping generation"
        result = detect_soft_failure(content)
        assert result is not None
        assert "skipping" in result.lower()

    @pytest.mark.unit
    def test_nothing_to_evaluate(self):
        result = detect_soft_failure("nothing to evaluate for this chunk")
        assert result is not None

    @pytest.mark.unit
    def test_zero_samples(self):
        result = detect_soft_failure("0 samples to process in chunk 5")
        assert result is not None

    @pytest.mark.unit
    def test_all_already_completed(self):
        result = detect_soft_failure("all 500 examples already completed")
        assert result is not None

    @pytest.mark.unit
    def test_genuine_failure_not_soft(self):
        content = (
            "Loading model...\n"
            "RuntimeError: CUDA out of memory\n"
        )
        assert detect_soft_failure(content) is None

    @pytest.mark.unit
    def test_empty_and_none(self):
        assert detect_soft_failure("") is None
        assert detect_soft_failure(None) is None

    @pytest.mark.unit
    def test_clean_log_not_soft(self):
        content = "Epoch 1/10 complete. Loss: 0.42"
        assert detect_soft_failure(content) is None


class TestCrashAndSoftFailInteraction:
    """The key semantic: when both crash and soft-fail are detected,
    soft-fail wins (the crash is collateral from the skip)."""

    @pytest.mark.unit
    def test_nemo_retry_pattern(self):
        """Real-world NeMo-Skills retry log: generation skipped,
        eval crashes on missing chunk files."""
        content = (
            "Waiting for the server to start at localhost:5000\n"
            "Successfully connected to server.\n"
            "File `/data/output.jsonl` exists, skipping generation\n"
            "No data to process, exiting.\n"
            "Error executing job with overrides: ['++input_files=...']\n"
            "Traceback (most recent call last):\n"
            "  File \"/nemo_run/code/evaluate_results.py\", line 104\n"
            "ValueError: No files found with the given pattern.\n"
        )
        crash = detect_crash(content)
        soft = detect_soft_failure(content)
        assert crash is not None, "crash should be detected"
        assert soft is not None, "soft-fail should also be detected"

    @pytest.mark.unit
    def test_genuine_crash_no_soft(self):
        """Real crash without skip indicators — should NOT be soft."""
        content = (
            "Loading model weights...\n"
            "Processing chunk 5 of 50\n"
            "Traceback (most recent call last):\n"
            "  File \"generate.py\", line 42\n"
            "RuntimeError: CUDA out of memory\n"
        )
        crash = detect_crash(content)
        soft = detect_soft_failure(content)
        assert crash is not None
        assert soft is None


class TestToolCallFalsePositive:
    """Tracebacks inside tool call responses are soft fails, not crashes."""

    @pytest.mark.unit
    def test_single_line_tool_call_with_traceback(self):
        content = (
            "Remaining generations:  95%|█████████▍| 73/77\n"
            "2026-03-28 14:08:24 INFO  Sending tool calls: [{'role': 'tool', 'name': 'stateful_python_code_exec', "
            "'tool_call_id': 'call_abc123', 'content': 'Traceback (most recent call last):\\n"
            "    all_atoms = np.array(all_atoms)\\nValueError: setting an array element with a sequence.'}]\n"
            "Remaining generations: 100%|██████████| 77/77\n"
        )
        assert detect_crash(content) is None

    @pytest.mark.unit
    def test_rich_console_tool_call_with_traceback(self):
        content = (
            "Remaining generations:  95%|█████████▍| 73/77\n"
            "[03/28/26 14:08:24] INFO     Sending tool calls: [{'role':      tool_call.py:172\n"
            "                             'tool', 'name':\n"
            "                             'stateful_python_code_exec',\n"
            "                             'content': 'Traceback (most recent call last):\\n\n"
            "                             ValueError: bad value'}]\n"
            "Remaining generations: 100%|██████████| 77/77\n"
        )
        assert detect_crash(content) is None

    @pytest.mark.unit
    def test_tool_call_py_source_lines_stripped(self):
        content = (
            "2026-03-28 14:05:00 INFO  some normal log\n"
            "[03/28/26 14:05:31] INFO     Sending tool calls: [{'role':      tool_call.py:172\n"
            "                             'content': 'Traceback (most recent call last):\\n\n"
            "                             RuntimeError: CUDA error'}]\n"
            "2026-03-28 14:05:32 INFO  generation complete\n"
        )
        assert detect_crash(content) is None

    @pytest.mark.unit
    def test_real_crash_still_detected_alongside_tool_calls(self):
        content = (
            "2026-03-28 14:05:00 INFO  Sending tool calls: [{'content': 'ValueError: bad input'}]\n"
            "2026-03-28 14:06:00 INFO  Processing batch 5\n"
            "Traceback (most recent call last):\n"
            '  File "train.py", line 42\n'
            "RuntimeError: CUDA out of memory\n"
        )
        crash = detect_crash(content)
        assert crash is not None
        assert "Traceback" in crash or "CUDA" in crash

    @pytest.mark.unit
    def test_tool_call_limit_not_flagged(self):
        content = (
            "2026-03-28 14:05:50 INFO  Tool call limit reached (max_tool_calls=10); stopping generation.\n"
            "Remaining generations: 100%|██████████| 77/77\n"
        )
        assert detect_crash(content) is None


class TestToolCallSoftFail:
    """When crashes exist only in tool call responses, detect_soft_failure
    should return a tool-call reason so the job is labeled SOFT FAIL."""

    @pytest.mark.unit
    def test_tool_call_only_traceback_is_soft_fail(self):
        content = (
            "2026-03-28 14:05:00 INFO  Processing batch 5\n"
            "2026-03-28 14:05:31 INFO  Sending tool calls: [{'role': 'tool', 'name': 'stateful_python_code_exec', "
            "'content': 'Traceback (most recent call last):\\n    x = bad()\\nValueError: bad value'}]\n"
            "2026-03-28 14:05:50 INFO  Tool call limit reached (max_tool_calls=10); stopping generation.\n"
            "Remaining generations: 100%|██████████| 77/77\n"
        )
        soft = detect_soft_failure(content)
        assert soft is not None
        assert "tool call" in soft.lower()

    @pytest.mark.unit
    def test_real_crash_not_soft_fail(self):
        content = (
            "Processing batch 5\n"
            "Traceback (most recent call last):\n"
            '  File "train.py", line 42\n'
            "RuntimeError: CUDA out of memory\n"
        )
        soft = detect_soft_failure(content)
        assert soft is None

    @pytest.mark.unit
    def test_mixed_real_and_tool_call_crash_not_soft(self):
        content = (
            "2026-03-28 14:05:31 INFO  Sending tool calls: [{'content': 'ValueError: bad'}]\n"
            "2026-03-28 14:06:00 INFO  Processing batch\n"
            "Traceback (most recent call last):\n"
            '  File "train.py", line 42\n'
            "RuntimeError: CUDA out of memory\n"
        )
        soft = detect_soft_failure(content)
        assert soft is None

    @pytest.mark.unit
    def test_clean_log_not_soft_fail(self):
        content = "Epoch 1/10 complete. Loss: 0.42\nTraining done."
        soft = detect_soft_failure(content)
        assert soft is None


class TestIsBenignLine:
    @pytest.mark.unit
    def test_sandbox_restoration(self):
        assert is_benign_line("sandbox state restoration failed — retrying")

    @pytest.mark.unit
    def test_sandbox_communication(self):
        assert is_benign_line("sandbox communication error on port 8080")

    @pytest.mark.unit
    def test_tool_call_response(self):
        assert is_benign_line("sending tool calls: [{'role': 'tool', 'content': 'traceback...'}]")

    @pytest.mark.unit
    def test_normal_line(self):
        assert not is_benign_line("training step 100 complete")
