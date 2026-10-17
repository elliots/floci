"""Immutable identities and pre-start DNS for an existing protected environment."""
import ipaddress
import json
from pathlib import Path
import re


METADATA = "/etc/floci/network-policy.json"
RESOLVER = "/etc/floci-resolv.conf"


def inspect(command, network, helper, dns):
    address = ipaddress.IPv4Address(dns)
    if address.is_unspecified or address.is_multicast or address.is_loopback or str(address) == "255.255.255.255":
        raise ValueError("environment-dns must be an assigned unicast IPv4 address")
    networks = json.loads(command("docker", "network", "inspect", network))
    helpers = json.loads(command("docker", "inspect", helper))
    if len(networks) != 1 or len(helpers) != 1:
        raise RuntimeError("environment network and isolation helper must each resolve to one object")
    network, helper = networks[0], helpers[0]
    network_id, helper_id = network.get("Id", ""), helper.get("Id", "")
    if not re.fullmatch("[0-9a-f]{64}", network_id) or not re.fullmatch("[0-9a-f]{64}", helper_id):
        raise RuntimeError("environment objects require complete immutable Docker identities")
    if network.get("Driver") != "bridge" or network.get("Scope") != "local":
        raise RuntimeError("environment network must be a local Docker bridge")
    assigned = [value.get("IPv4Address", "").split("/", 1)[0] for value in network.get("Containers", {}).values()]
    if str(address) not in assigned:
        raise RuntimeError("environment-dns must belong to a container on the protected network")
    labels = helper.get("Config", {}).get("Labels") or {}
    owner, table = labels.get("floci.network.owner"), labels.get("floci.network.table", "")
    if not owner or not re.fullmatch("floci_iso_[a-z0-9]+", table):
        raise RuntimeError("isolation helper is missing valid Floci ownership labels")
    if not helper.get("State", {}).get("Running") or helper.get("HostConfig", {}).get("NetworkMode") != "host":
        raise RuntimeError("isolation helper must be running in the Docker host network")
    metadata = json.loads(command("docker", "exec", helper_id, "cat", METADATA))
    if metadata.get("version") != 1 or metadata.get("owner") != owner or metadata.get("table") != table:
        raise RuntimeError("isolation helper metadata does not match its ownership labels")
    fingerprint = metadata.get("fingerprint", "")
    if not re.fullmatch("[0-9a-f]{64}", fingerprint):
        raise RuntimeError("isolation helper has an invalid policy fingerprint")
    bridge = (network.get("Options") or {}).get("com.docker.network.bridge.name", "br-" + network_id[:12])
    expected = {"name": network["Name"], "networkId": network_id, "bridge": bridge}
    if expected not in metadata.get("bridges", []):
        raise RuntimeError("isolation helper does not protect the selected Docker bridge")
    return {"networkName": network["Name"], "networkID": network_id, "helperID": helper_id,
        "owner": owner, "table": table, "fingerprint": fingerprint, "bridge": bridge, "dns": str(address)}


def resolve(record, args, command, existing):
    values = [getattr(args, name, None) for name in ("environment_network", "isolation_helper", "environment_dns")]
    if any(values) and not all(values):
        raise ValueError("environment-network, isolation-helper and environment-dns must be supplied together")
    previous = record.get("environment")
    if not any(values):
        if not previous:
            return
        values = [previous["networkID"], previous["helperID"], previous["dns"]]
    if existing and not previous:
        raise RuntimeError("an existing unisolated cluster cannot change environment; prepare a new private directory")
    observed = inspect(command, *values)
    if previous is not None and observed != previous:
        raise RuntimeError("protected environment identity changed; refusing to adopt replacement network, helper or policy")
    record["environment"] = observed
    record["version"] = 2


def verify_peer(record, peer):
    environment = record.get("environment")
    if not environment:
        return
    networks = list(peer.get("NetworkSettings", {}).get("Networks", {}).values())
    if len(networks) != 1 or networks[0].get("NetworkID") != environment["networkID"]:
        raise RuntimeError("network peer must remain attached only to its recorded protected network")
    if peer.get("HostConfig", {}).get("Dns") != [environment["dns"]]:
        raise RuntimeError("network peer DNS differs from its protected environment")
    entries = [entry for entry in peer.get("Config", {}).get("Env", []) if entry.startswith("K3S_RESOLV_CONF=")]
    if entries != ["K3S_RESOLV_CONF=" + RESOLVER]:
        raise RuntimeError("network peer is missing its controlled Kubernetes resolver")


def peer_options(record):
    environment = record.get("environment")
    if not environment:
        return []
    return ["--network", environment["networkID"], "--dns", environment["dns"],
        "--env", "K3S_RESOLV_CONF=" + RESOLVER]


def stage_resolver(record, command, write_private):
    if environment := record.get("environment"):
        path = Path(record["directory"]) / "environment-resolv.conf"
        write_private(path, "nameserver " + environment["dns"] + "\noptions timeout:2 attempts:2\n")
        command("docker", "cp", str(path), record["peer"] + ":" + RESOLVER)


def controller_options(record):
    if environment := record.get("environment"):
        return ["--environment-network", environment["networkID"], "--isolation-helper", environment["helperID"],
            "--environment-dns", environment["dns"]]
    return []
