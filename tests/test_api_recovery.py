"""Tests for Samil Power API recovery behavior."""

from __future__ import annotations

import sys
import types
from typing import Any
from importlib import util

import pytest


class _ImmediateLoop:
    async def run_in_executor(self, _executor: Any, func: Any, *args: Any) -> Any:
        return func(*args)


class _FakeInverter:
    def __init__(self, serial: str, responses: list[Any]) -> None:
        self._serial = serial
        self._responses = list(responses)
        self._index = 0
        self.disconnected = False

    def model(self) -> dict[str, Any]:
        return {"serial_number": self._serial, "model_name": "Fake"}

    def status(self) -> dict[str, Any]:
        if self._index >= len(self._responses):
            response = self._responses[-1]
        else:
            response = self._responses[self._index]
            self._index += 1
        if isinstance(response, Exception):
            raise response
        return response

    def disconnect(self) -> None:
        self.disconnected = True


@pytest.fixture
def api_module(monkeypatch: pytest.MonkeyPatch):
    """Import the API module with a fake samil dependency."""
    inverter_module = types.ModuleType("samil.inverter")
    inverter_module.InverterFinder = object
    inverter_module.KeepAliveInverter = object
    inverter_module.InverterNotFoundError = type("InverterNotFoundError", (Exception,), {})
    inverterutil_module = types.ModuleType("samil.inverterutil")
    inverterutil_module.connect_inverters = lambda *args, **kwargs: []
    samil_module = types.ModuleType("samil")

    monkeypatch.setitem(sys.modules, "samil", samil_module)
    monkeypatch.setitem(sys.modules, "samil.inverter", inverter_module)
    monkeypatch.setitem(sys.modules, "samil.inverterutil", inverterutil_module)

    package_path = "/home/runner/work/ha_samil_power/ha_samil_power/custom_components/samil_power"
    custom_components_pkg = types.ModuleType("custom_components")
    custom_components_pkg.__path__ = ["/home/runner/work/ha_samil_power/ha_samil_power/custom_components"]
    samil_power_pkg = types.ModuleType("custom_components.samil_power")
    samil_power_pkg.__path__ = [package_path]
    monkeypatch.setitem(sys.modules, "custom_components", custom_components_pkg)
    monkeypatch.setitem(sys.modules, "custom_components.samil_power", samil_power_pkg)

    const_spec = util.spec_from_file_location(
        "custom_components.samil_power.const",
        f"{package_path}/const.py",
    )
    const_module = util.module_from_spec(const_spec)
    assert const_spec is not None and const_spec.loader is not None
    sys.modules["custom_components.samil_power.const"] = const_module
    const_spec.loader.exec_module(const_module)

    api_spec = util.spec_from_file_location(
        "custom_components.samil_power.api",
        f"{package_path}/api.py",
    )
    api = util.module_from_spec(api_spec)
    assert api_spec is not None and api_spec.loader is not None
    sys.modules["custom_components.samil_power.api"] = api
    api_spec.loader.exec_module(api)

    monkeypatch.setattr(api.asyncio, "get_event_loop", lambda: _ImmediateLoop())
    return api


@pytest.mark.asyncio
async def test_one_inverter_failure_does_not_block_others(api_module):
    """If one inverter times out, healthy inverters should still update."""
    client = api_module.SamilPowerApiClient(inverters=2)
    client._connected = True
    client._reconnect_backoff_seconds = 0.0

    ok = _FakeInverter("SN-OK", [{"output_power": 100}])
    failing = _FakeInverter("SN-BAD", [RuntimeError("timeout")])
    client._inverters = {0: ok, 1: failing}
    client._model_info = {
        0: {"serial_number": "SN-OK"},
        1: {"serial_number": "SN-BAD"},
    }
    client._serial_to_index = {"SN-OK": 0, "SN-BAD": 1}

    async def _no_reconnect(*_args, **_kwargs):
        return None

    client.async_connect = _no_reconnect

    data = await client.async_get_data()

    assert set(data) == {0}
    assert data[0]["status"]["output_power"] == 100
    assert 1 not in client._inverters


@pytest.mark.asyncio
async def test_failed_inverter_recovers_on_later_update(api_module):
    """A failed inverter should reconnect independently on later poll."""
    client = api_module.SamilPowerApiClient(inverters=2)
    client._connected = True
    client._reconnect_backoff_seconds = 0.0

    ok = _FakeInverter("SN-OK", [{"output_power": 110}, {"output_power": 120}])
    failing = _FakeInverter("SN-RECOVER", [RuntimeError("offline")])
    recovered = _FakeInverter("SN-RECOVER", [{"output_power": 210}])

    client._inverters = {0: ok, 1: failing}
    client._model_info = {
        0: {"serial_number": "SN-OK"},
        1: {"serial_number": "SN-RECOVER"},
    }
    client._serial_to_index = {"SN-OK": 0, "SN-RECOVER": 1}

    reconnect_state = {"calls": 0}

    async def _reconnect(force: bool = False):
        reconnect_state["calls"] += 1
        if force and reconnect_state["calls"] >= 2:
            client._inverters[1] = recovered
            client._model_info[1] = recovered.model()
            client._connected = True

    client.async_connect = _reconnect

    first_data = await client.async_get_data()
    assert set(first_data) == {0}

    second_data = await client.async_get_data()
    assert set(second_data) == {0, 1}
    assert second_data[0]["status"]["output_power"] == 120
    assert second_data[1]["status"]["output_power"] == 210
