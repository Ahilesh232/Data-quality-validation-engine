import pytest
import yaml

from dq_engine.config import ConfigError, load_config, parse_config
from tests.conftest import ROOT


def _raw():
    return yaml.safe_load(open(ROOT / "config" / "pipeline.yaml"))


def test_shipped_config_loads_and_adds_required_rules():
    cfg = load_config(ROOT / "config" / "pipeline.yaml")
    ids = {r.id for r in cfg.dataset("customers").rules}
    assert "customers_email_required" in ids          # auto-generated from nullable:false


@pytest.mark.parametrize("mutate,match", [
    (lambda r: r["datasets"][0]["rules"][0].update(severity="fatal"), "severity"),
    (lambda r: r["datasets"][0]["rules"][0].update(type="magic"), "unknown rule type"),
    (lambda r: r["datasets"][0]["columns"][0].update(name="BadName"), "snake_case"),
    (lambda r: r["datasets"][0]["rules"][1].update(expression="1=1; DROP TABLE x"), "one SQL expression"),
    (lambda r: r["datasets"][1]["rules"][1].update(ref_dataset="ghost"), "declared earlier"),
    (lambda r: r["datasets"][0].update(key=["nope"]), "key must list"),
])
def test_invalid_config_is_rejected(mutate, match):
    raw = _raw()
    mutate(raw)
    with pytest.raises(ConfigError, match=match):
        parse_config(raw)
