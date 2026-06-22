"""Ensures all module-level flow functions are compatible with Prefect deployments.

When a flow is triggered via a Prefect deployment, Prefect calls:
    get_call_parameters(flow.fn, call_args, call_kwargs)
which internally calls inspect.signature(flow.fn).bind(**kwargs).
If flow.fn has 'self' in its signature (because it is an unbound method), Prefect
raises ParameterBindError: missing a required argument: 'self'.
"""

import inspect

import pytest

from prefect_server.pollHiEsaStep import (
    poll_hi45_esa_step_flow,
    poll_hi90_esa_step_flow,
)
from prefect_server.pollHK import poll_hk_flow
from prefect_server.pollIALiRT import poll_ialirt_flow, poll_ialirt_hk_flow
from prefect_server.pollLoPivotPlatform import poll_lo_pivot_platform_flow
from prefect_server.pollScience import poll_science_flow
from prefect_server.pollSmallForces import poll_small_forces_flow
from prefect_server.pollSpice import poll_spice_flow
from prefect_server.pollSpinTable import poll_spin_table_flow

ALL_FLOWS = [
    poll_hi45_esa_step_flow,
    poll_hi90_esa_step_flow,
    poll_hk_flow,
    poll_ialirt_flow,
    poll_ialirt_hk_flow,
    poll_lo_pivot_platform_flow,
    poll_science_flow,
    poll_small_forces_flow,
    poll_spice_flow,
    poll_spin_table_flow,
]


@pytest.mark.parametrize("prefect_flow", ALL_FLOWS, ids=lambda f: f.name)
def test_flow_fn_has_no_self_parameter(prefect_flow):
    """Flow.fn must not have 'self' in its signature.

    If a class method is wrapped with flow(self._method), Prefect extracts the
    unbound function (which has 'self') and fails at deployment trigger time with
    ParameterBindError: missing a required argument: 'self'.
    """
    sig = inspect.signature(prefect_flow.fn)
    assert "self" not in sig.parameters, (
        f"'{prefect_flow.name}'.fn has 'self' in its signature — "
        "Prefect deployments will fail with ParameterBindError. "
        "Fix: use a module-level @flow function instead of flow(self._method)."
    )
