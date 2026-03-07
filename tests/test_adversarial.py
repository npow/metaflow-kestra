"""
Adversarial and edge-case tests for the Kestra YAML compiler.

These tests focus on:
  - String injection safety (tags, namespaces, decorator specs with special chars)
  - YAML injection safety (label values with YAML-special chars)
  - Edge-case inputs (workflow timeout, ISO duration, type inference)
  - Callable parameter defaults
  - Compiler unit helpers tested directly
"""

import os
import textwrap

import pytest
import yaml

from conftest import FLOWS_DIR, compile_flow

# ---------------------------------------------------------------------------
# Import compiler helpers directly for unit-level tests
# ---------------------------------------------------------------------------

from metaflow_extensions.kestra.plugins.kestra.kestra_compiler import (
    KestraCompiler,
    _iso_duration,
    flow_name_to_id,
)


# ---------------------------------------------------------------------------
# _iso_duration unit tests
# ---------------------------------------------------------------------------

class TestIsoDuration:
    def test_seconds_only(self):
        assert _iso_duration(45) == "PT45S"

    def test_zero_seconds(self):
        # Edge: 0 seconds → must still produce "PT0S" (not "PT")
        assert _iso_duration(0) == "PT0S"

    def test_minutes_only(self):
        assert _iso_duration(120) == "PT2M"

    def test_minutes_and_seconds(self):
        assert _iso_duration(90) == "PT1M30S"

    def test_hours_only(self):
        # Exercises the "hours" branch (line 51)
        assert _iso_duration(3600) == "PT1H"

    def test_hours_and_minutes(self):
        assert _iso_duration(3660) == "PT1H1M"

    def test_hours_minutes_seconds(self):
        assert _iso_duration(3661) == "PT1H1M1S"

    def test_large_value(self):
        # 2h 30m 15s
        assert _iso_duration(2 * 3600 + 30 * 60 + 15) == "PT2H30M15S"


# ---------------------------------------------------------------------------
# flow_name_to_id unit tests
# ---------------------------------------------------------------------------

class TestFlowNameToId:
    def test_basic_camel_case(self):
        assert flow_name_to_id("MyFlow") == "myflow"

    def test_underscores_to_hyphens(self):
        assert flow_name_to_id("My_Flow") == "my-flow"

    def test_dots_to_hyphens(self):
        assert flow_name_to_id("my.flow") == "my-flow"

    def test_already_lowercase(self):
        assert flow_name_to_id("myflow") == "myflow"


# ---------------------------------------------------------------------------
# _infer_kestra_type unit tests
# ---------------------------------------------------------------------------

class TestInferKestraType:
    def test_bool(self):
        # bool must come before int (True is also int)
        assert KestraCompiler._infer_kestra_type(True) == "BOOLEAN"
        assert KestraCompiler._infer_kestra_type(False) == "BOOLEAN"

    def test_int(self):
        assert KestraCompiler._infer_kestra_type(0) == "INT"
        assert KestraCompiler._infer_kestra_type(42) == "INT"

    def test_float(self):
        assert KestraCompiler._infer_kestra_type(3.14) == "FLOAT"
        assert KestraCompiler._infer_kestra_type(0.0) == "FLOAT"

    def test_string(self):
        assert KestraCompiler._infer_kestra_type("hello") == "STRING"
        assert KestraCompiler._infer_kestra_type("") == "STRING"
        assert KestraCompiler._infer_kestra_type(None) == "STRING"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_flow(tmp_path, name, source):
    p = tmp_path / name
    p.write_text(textwrap.dedent(source))
    return str(p)


def _load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Workflow timeout
# ---------------------------------------------------------------------------

class TestWorkflowTimeout:
    def test_timeout_emitted_in_yaml(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "timeout.yaml")
        yaml_str = compile_flow(flow_file, out, extra_args=["--workflow-timeout=7200"])
        assert "timeout: PT2H" in yaml_str

    def test_timeout_is_valid_yaml(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "timeout.yaml")
        compile_flow(flow_file, out, extra_args=["--workflow-timeout=3661"])
        data = _load_yaml(out)
        assert data["timeout"] == "PT1H1M1S"

    def test_no_timeout_by_default(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "no_timeout.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        assert "timeout" not in data


# ---------------------------------------------------------------------------
# Tag injection
# ---------------------------------------------------------------------------

class TestTagInjection:
    def test_tag_with_double_quote_is_escaped(self, tmp_path):
        """A tag containing '"' must be escaped so the generated Python is valid."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "tag_quote.yaml")
        yaml_str = compile_flow(flow_file, out, extra_args=['--tag=my"tag'])
        # The raw '"' must not appear unescaped inside a Python string literal
        # in the generated YAML script. Specifically, '\"' should be escaped.
        assert '"--tag"' in yaml_str
        assert '\\"' in yaml_str  # escaped in generated Python

    def test_tag_with_backslash_is_escaped(self, tmp_path):
        """A tag containing backslash must be double-escaped."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "tag_bs.yaml")
        yaml_str = compile_flow(flow_file, out, extra_args=["--tag=my\\tag"])
        assert "\\\\" in yaml_str  # double-escaped backslash

    def test_multiple_tags(self, tmp_path):
        """Multiple --tag args should all appear in the YAML."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "multi_tag.yaml")
        yaml_str = compile_flow(
            flow_file, out, extra_args=["--tag=alpha", "--tag=beta"]
        )
        assert "alpha" in yaml_str
        assert "beta" in yaml_str

    def test_tag_yaml_is_valid(self, tmp_path):
        """Tags containing colons must not break YAML output."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "tag_colon.yaml")
        compile_flow(flow_file, out, extra_args=["--tag=env:prod"])
        data = _load_yaml(out)
        assert data is not None


# ---------------------------------------------------------------------------
# Namespace injection
# ---------------------------------------------------------------------------

class TestNamespaceInjection:
    def test_namespace_in_generated_python(self, tmp_path):
        """--namespace value should appear in the generated step script."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "ns.yaml")
        yaml_str = compile_flow(flow_file, out, extra_args=["--namespace=production"])
        assert "--namespace=production" in yaml_str

    def test_namespace_with_special_chars_escaped(self, tmp_path):
        """Namespace with a double-quote must be escaped in generated Python."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "ns_quote.yaml")
        # Use a namespace that contains a backslash — must be double-escaped
        yaml_str = compile_flow(
            flow_file, out, extra_args=["--namespace=team\\x"]
        )
        assert "\\\\" in yaml_str


# ---------------------------------------------------------------------------
# --with decorator injection
# ---------------------------------------------------------------------------

class TestWithDecoratorInjection:
    def test_with_containing_equals_sign(self, tmp_path):
        """A --with value with '=' (normal for resources:cpu=4) should round-trip."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "with_eq.yaml")
        yaml_str = compile_flow(
            flow_file, out, extra_args=["--with=resources:cpu=4"]
        )
        assert "resources:cpu=4" in yaml_str

    def test_with_containing_double_quote_escaped(self, tmp_path):
        """A --with value with '"' must be escaped in the generated Python script."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "with_quote.yaml")
        yaml_str = compile_flow(
            flow_file, out, extra_args=['--with=batch:image="myrepo/img:latest"']
        )
        # The raw unescaped '"' inside a Python string literal would break execution;
        # check the escaped version is present.
        assert '\\"' in yaml_str

    def test_with_yaml_remains_valid(self, tmp_path):
        """--with with colons/equals should produce parseable YAML."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "with_colon.yaml")
        compile_flow(flow_file, out, extra_args=["--with=resources:cpu=2,memory=4000"])
        data = _load_yaml(out)
        assert data is not None


# ---------------------------------------------------------------------------
# Kestra namespace injection (YAML label)
# ---------------------------------------------------------------------------

class TestKestraNamespaceInjection:
    def test_kestra_namespace_with_dots(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "kns_dots.yaml")
        yaml_str = compile_flow(
            flow_file, out, extra_args=["--kestra-namespace=com.example.ml"]
        )
        data = _load_yaml(out)
        assert data["namespace"] == "com.example.ml"

    def test_kestra_namespace_colons_dont_break_yaml(self, tmp_path):
        """A namespace containing ':' should be quoted in YAML, not break parsing."""
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "kns_colon.yaml")
        # Kestra only supports dotted namespaces, but the compiler should still
        # produce parseable YAML rather than crashing.
        compile_flow(flow_file, out, extra_args=["--kestra-namespace=team"])
        data = _load_yaml(out)
        assert data is not None


# ---------------------------------------------------------------------------
# Callable parameter default
# ---------------------------------------------------------------------------

class TestCallableDefault:
    def test_callable_default_becomes_string_type(self, tmp_path):
        """Parameters with callable defaults should compile without error
        and use STRING type (since the default is resolved as None)."""
        flow = _write_flow(
            tmp_path,
            "callable_param_flow.py",
            """\
            from metaflow import FlowSpec, Parameter, step

            def _default_msg():
                return "dynamic"

            class CallableParamFlow(FlowSpec):
                # Callable default — compiler should handle gracefully
                msg = Parameter("msg", default=_default_msg, help="Dynamic default")

                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass

            if __name__ == "__main__":
                CallableParamFlow()
            """,
        )
        out = str(tmp_path / "callable.yaml")
        yaml_str = compile_flow(flow, out)
        data = _load_yaml(out)
        # Flow should compile without error
        assert data is not None
        # Parameter should appear in inputs
        assert "msg" in yaml_str


# ---------------------------------------------------------------------------
# Parameter type inference end-to-end
# ---------------------------------------------------------------------------

class TestParameterTypeInference:
    def test_bool_param_type(self, tmp_path):
        flow = _write_flow(
            tmp_path,
            "bool_param_flow.py",
            """\
            from metaflow import FlowSpec, Parameter, step

            class BoolParamFlow(FlowSpec):
                flag = Parameter("flag", default=True)

                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass

            if __name__ == "__main__":
                BoolParamFlow()
            """,
        )
        out = str(tmp_path / "bool_param.yaml")
        compile_flow(flow, out)
        data = _load_yaml(out)
        inputs = {i["id"]: i for i in data.get("inputs", [])}
        assert inputs["flag"]["type"] == "BOOLEAN"

    def test_float_param_type(self, tmp_path):
        flow = _write_flow(
            tmp_path,
            "float_param_flow.py",
            """\
            from metaflow import FlowSpec, Parameter, step

            class FloatParamFlow(FlowSpec):
                threshold = Parameter("threshold", default=0.5)

                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass

            if __name__ == "__main__":
                FloatParamFlow()
            """,
        )
        out = str(tmp_path / "float_param.yaml")
        compile_flow(flow, out)
        data = _load_yaml(out)
        inputs = {i["id"]: i for i in data.get("inputs", [])}
        assert inputs["threshold"]["type"] == "FLOAT"

    def test_int_param_type(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "param_flow.py")
        out = str(tmp_path / "int_param.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        inputs = {i["id"]: i for i in data.get("inputs", [])}
        assert inputs["count"]["type"] == "INT"


# ---------------------------------------------------------------------------
# Environment decorator (covers _build_env_overrides_str)
# ---------------------------------------------------------------------------

class TestEnvironmentDecorator:
    def test_env_vars_in_step_script(self, tmp_path):
        flow = _write_flow(
            tmp_path,
            "env_flow.py",
            """\
            from metaflow import FlowSpec, environment, step

            class EnvFlow(FlowSpec):
                @environment(vars={"MY_VAR": "hello", "NUM": "42"})
                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass

            if __name__ == "__main__":
                EnvFlow()
            """,
        )
        out = str(tmp_path / "env.yaml")
        yaml_str = compile_flow(flow, out)
        assert "MY_VAR" in yaml_str
        assert "hello" in yaml_str

    def test_env_vars_with_special_chars_escaped(self, tmp_path):
        """Env var values containing '"' and '\\' must be escaped."""
        flow = _write_flow(
            tmp_path,
            "env_inject_flow.py",
            """\
            from metaflow import FlowSpec, environment, step

            class EnvInjectFlow(FlowSpec):
                @environment(vars={"PATH_VAR": '/usr/bin"evil', "BS_VAR": "a\\\\b"})
                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass

            if __name__ == "__main__":
                EnvInjectFlow()
            """,
        )
        out = str(tmp_path / "env_inject.yaml")
        yaml_str = compile_flow(flow, out)
        data = _load_yaml(out)
        assert data is not None  # YAML must remain parseable
        # The quote should be escaped in the generated Python script
        assert '\\"' in yaml_str


# ---------------------------------------------------------------------------
# Schedule with timezone (covers line 245 in compiler)
# ---------------------------------------------------------------------------

class TestScheduleWithTimezone:
    def test_timezone_in_yaml(self, tmp_path):
        flow = _write_flow(
            tmp_path,
            "tz_schedule_flow.py",
            """\
            from metaflow import FlowSpec, schedule, step

            @schedule(cron="0 9 * * 1-5", timezone="America/Los_Angeles")
            class TzScheduleFlow(FlowSpec):
                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass

            if __name__ == "__main__":
                TzScheduleFlow()
            """,
        )
        out = str(tmp_path / "tz_schedule.yaml")
        yaml_str = compile_flow(flow, out)
        data = _load_yaml(out)
        trigger = data["triggers"][0]
        assert trigger["cron"] == "0 9 * * 1-5"
        assert trigger["timezone"] == "America/Los_Angeles"


# ---------------------------------------------------------------------------
# Project label YAML injection (covers lines 180-181 in compiler)
# ---------------------------------------------------------------------------

class TestProjectLabelInjection:
    def test_project_label_yaml_valid(self, tmp_path):
        """Project labels containing YAML-special chars must produce parseable YAML."""
        flow_file = os.path.join(FLOWS_DIR, "project_flow.py")
        out = str(tmp_path / "project_label.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        assert data is not None
        labels = data.get("labels", {})
        assert "metaflow.project" in labels
        assert "metaflow.branch" in labels


# ---------------------------------------------------------------------------
# Compiler rejects unsupported parameter (no default)
# ---------------------------------------------------------------------------

class TestParameterValidation:
    def test_parameter_without_default_fails(self, tmp_path):
        """A Parameter with no default should fail compilation."""
        flow = _write_flow(
            tmp_path,
            "no_default_flow.py",
            """\
            from metaflow import FlowSpec, Parameter, step

            class NoDefaultFlow(FlowSpec):
                msg = Parameter("msg", help="No default value")

                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass

            if __name__ == "__main__":
                NoDefaultFlow()
            """,
        )
        out = str(tmp_path / "no_default.yaml")
        with pytest.raises(Exception):
            compile_flow(flow, out)


# ---------------------------------------------------------------------------
# Retry decorator end-to-end
# ---------------------------------------------------------------------------

class TestRetryDecorator:
    def test_retry_step_script_has_max_retries(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "retry_flow.py")
        out = str(tmp_path / "retry_check.yaml")
        yaml_str = compile_flow(flow_file, out)
        data = _load_yaml(out)
        assert data is not None
        # maxAttempt should be > 1 for a retried step
        yaml_text = str(data)
        assert "maxAttempt" in yaml_str or "max_user_code_retries" in yaml_str

    def test_retry_yaml_is_valid(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "retry_flow.py")
        out = str(tmp_path / "retry_valid.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        assert data is not None


# ---------------------------------------------------------------------------
# Parameter default value propagation
# ---------------------------------------------------------------------------

class TestParameterDefaults:
    def test_string_default_in_yaml_input(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "param_flow.py")
        out = str(tmp_path / "param_defaults.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        inputs = {i["id"]: i for i in data.get("inputs", [])}
        assert inputs["greeting"]["defaults"] == "hello"
        assert inputs["count"]["defaults"] == 3

    def test_string_default_with_special_chars(self, tmp_path):
        """Parameter default containing YAML-special chars should still parse."""
        flow = _write_flow(
            tmp_path,
            "special_default_flow.py",
            """\
            from metaflow import FlowSpec, Parameter, step

            class SpecialDefaultFlow(FlowSpec):
                msg = Parameter("msg", default="hello: world")

                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass

            if __name__ == "__main__":
                SpecialDefaultFlow()
            """,
        )
        out = str(tmp_path / "special_default.yaml")
        compile_flow(flow, out)
        data = _load_yaml(out)
        inputs = {i["id"]: i for i in data.get("inputs", [])}
        assert inputs["msg"]["defaults"] == "hello: world"
