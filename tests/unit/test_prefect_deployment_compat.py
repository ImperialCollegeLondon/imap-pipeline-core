"""Ensures all class-based flows expose @flow-decorated run methods.

The pattern used in this project is: @flow decorates the run method directly on
the class. Prefect handles the descriptor protocol when called as instance.run().
Deployments are created via SomeFlow().run.to_deployment(...).
"""

import pytest

from prefect_server.checkIALiRT import CheckIALiRTFlow
from prefect_server.pollHiEsaStep import (
    poll_hi45_esa_step_flow,
    poll_hi90_esa_step_flow,
)
from prefect_server.pollHK import PollHKFlow
from prefect_server.pollIALiRT import PollIALiRTFlow
from prefect_server.pollLoPivotPlatform import poll_lo_pivot_platform_flow
from prefect_server.pollScience import PollScienceFlow
from prefect_server.pollSmallForces import PollSmallForcesFlow
from prefect_server.pollSpice import PollSpiceFlow
from prefect_server.pollSpinTable import PollSpinTableFlow
from prefect_server.quicklookIALiRT import QuicklookIALiRTFlow

CLASS_BASED_FLOWS = [
    (CheckIALiRTFlow, "run"),
    (PollHKFlow, "run"),
    (PollIALiRTFlow, "run"),
    (PollIALiRTFlow, "run_hk"),
    (PollScienceFlow, "run"),
    (PollSmallForcesFlow, "run"),
    (PollSpiceFlow, "run"),
    (PollSpinTableFlow, "run"),
    (QuicklookIALiRTFlow, "run"),
]

FUNCTIONAL_FLOWS = [
    poll_hi45_esa_step_flow,
    poll_hi90_esa_step_flow,
    poll_lo_pivot_platform_flow,
]


@pytest.mark.parametrize(
    "flow_cls,method", CLASS_BASED_FLOWS, ids=lambda x: getattr(x, "__name__", x)
)
def test_class_flow_has_flow_decorated_method(flow_cls, method):
    """Each class-based flow must expose a @flow-decorated method."""
    attr = getattr(flow_cls, method)
    assert hasattr(attr, "fn"), (
        f"{flow_cls.__name__}.{method} is not a Prefect @flow object — "
        "missing the @flow decorator"
    )


@pytest.mark.parametrize(
    "flow_cls,method", CLASS_BASED_FLOWS, ids=lambda x: getattr(x, "__name__", x)
)
def test_class_flow_instance_has_flow_method(flow_cls, method):
    """A fresh instance must expose the same @flow object."""
    instance = flow_cls()
    attr = getattr(instance, method)
    assert hasattr(attr, "fn"), (
        f"{flow_cls.__name__}().{method} is not accessible as a @flow object on an instance"
    )


@pytest.mark.parametrize("prefect_flow", FUNCTIONAL_FLOWS, ids=lambda f: f.name)
def test_functional_flow_fn_has_no_self_parameter(prefect_flow):
    """Module-level @flow functions must not have 'self' in their signature."""
    import inspect

    sig = inspect.signature(prefect_flow.fn)
    assert "self" not in sig.parameters, (
        f"'{prefect_flow.name}'.fn has 'self' in its signature — "
        "Prefect deployments will fail with ParameterBindError."
    )
