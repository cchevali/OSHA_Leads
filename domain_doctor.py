"""
Domain Doctor: Vercel + Cloudflare DNS diagnostics and fix-ups for a single project.

Default behavior is read-only and prints a concise report. Use --apply-dns to enforce
Cloudflare DNS records based on Vercel "recommended" values.

Security: tokens are read from environment variables only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import requests


VERCEL_API = "https://api.vercel.com"
CF_API = "https://api.cloudflare.com/client/v4"


DEFAULT_PROJECT_ID = "prj_4zNry4TmGzqQK1hVFMjqR0MMizT1"
DEFAULT_APEX = "microflowops.com"
DEFAULT_WWW = "www.microflowops.com"
DEFAULT_TTL = 1  # Cloudflare "Auto"


class DomainDoctorError(RuntimeError):
    pass


@dataclass(frozen=True)
class VercelRecommendation:
    apex_a: str | None
    www_cname: str | None
    apex_raw: dict[str, Any]
    www_raw: dict[str, Any]


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env(name: str, default: str = "", required: bool = False) -> str:
    value = os.getenv(name, default).strip()
    if required and not value:
        raise DomainDoctorError(f"Missing required env var: {name}")
    return value


def _http_json(resp: requests.Response) -> dict[str, Any]:
    try:
        payload = resp.json()
    except Exception:
        raise DomainDoctorError(f"HTTP {resp.status_code} non-JSON response: {resp.text[:300]}")
    if resp.status_code >= 400:
        msg = payload.get("error", {}).get("message") or payload.get("errors") or payload
        raise DomainDoctorError(f"HTTP {resp.status_code}: {msg}")
    return payload


def _vercel_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _cf_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def vercel_get_project(token: str, project_id: str, team_id: str = "") -> dict[str, Any]:
    url = f"{VERCEL_API}/v9/projects/{project_id}"
    params = {"teamId": team_id} if team_id else None
    resp = requests.get(url, headers=_vercel_headers(token), params=params, timeout=30)
    return _http_json(resp)


def vercel_list_project_domains(token: str, project_id: str, team_id: str = "") -> list[dict[str, Any]]:
    url = f"{VERCEL_API}/v9/projects/{project_id}/domains"
    params: dict[str, Any] = {"limit": 100}
    if team_id:
        params["teamId"] = team_id
    resp = requests.get(url, headers=_vercel_headers(token), params=params, timeout=30)
    payload = _http_json(resp)
    return list(payload.get("domains") or [])


def vercel_get_project_domain(token: str, project_id: str, domain: str, team_id: str = "") -> dict[str, Any] | None:
    url = f"{VERCEL_API}/v9/projects/{project_id}/domains/{domain}"
    params = {"teamId": team_id} if team_id else None
    resp = requests.get(url, headers=_vercel_headers(token), params=params, timeout=30)
    if resp.status_code == 404:
        return None
    return _http_json(resp)


def vercel_get_domain_config(
    token: str,
    domain: str,
    project_id_or_name: str,
    team_id: str = "",
) -> dict[str, Any]:
    url = f"{VERCEL_API}/v6/domains/{domain}/config"
    params: dict[str, Any] = {"projectIdOrName": project_id_or_name}
    if team_id:
        params["teamId"] = team_id
    resp = requests.get(url, headers=_vercel_headers(token), params=params, timeout=30)
    return _http_json(resp)


def _pick_recommended_ipv4(domain_config: dict[str, Any]) -> str | None:
    recs = domain_config.get("recommendedIPv4") or []
    if not isinstance(recs, list) or not recs:
        return None
    # Sort by smallest rank, then pick first value.
    def _rank(item: dict[str, Any]) -> int:
        try:
            return int(item.get("rank") or 999999)
        except Exception:
            return 999999

    best = sorted([r for r in recs if isinstance(r, dict)], key=_rank)[0]
    value = best.get("value")
    if isinstance(value, list) and value:
        return str(value[0]).strip() or None
    if isinstance(value, str):
        return value.strip() or None
    return None


def _pick_recommended_cname(domain_config: dict[str, Any]) -> str | None:
    recs = domain_config.get("recommendedCNAME") or []
    if not isinstance(recs, list) or not recs:
        return None
    def _rank(item: dict[str, Any]) -> int:
        try:
            return int(item.get("rank") or 999999)
        except Exception:
            return 999999

    best = sorted([r for r in recs if isinstance(r, dict)], key=_rank)[0]
    value = best.get("value")
    return str(value).strip() if value else None


def get_vercel_recommendations(
    token: str,
    project_id: str,
    apex: str,
    www: str,
    team_id: str = "",
) -> VercelRecommendation:
    apex_cfg = vercel_get_domain_config(token, apex, project_id, team_id=team_id)
    www_cfg = vercel_get_domain_config(token, www, project_id, team_id=team_id)
    return VercelRecommendation(
        apex_a=_pick_recommended_ipv4(apex_cfg),
        www_cname=_pick_recommended_cname(www_cfg),
        apex_raw=apex_cfg,
        www_raw=www_cfg,
    )


def cf_get_zone_id(token: str, zone_id: str, zone_name: str) -> str:
    if zone_id:
        return zone_id
    if not zone_name:
        raise DomainDoctorError("Provide CF_ZONE_ID or CF_ZONE_NAME")
    url = f"{CF_API}/zones"
    resp = requests.get(url, headers=_cf_headers(token), params={"name": zone_name, "per_page": 50}, timeout=30)
    payload = _http_json(resp)
    results = payload.get("result") or []
    if not results:
        raise DomainDoctorError(f"Cloudflare zone not found for name={zone_name}")
    return str(results[0]["id"])


def cf_list_dns_records(token: str, zone_id: str, name: str) -> list[dict[str, Any]]:
    url = f"{CF_API}/zones/{zone_id}/dns_records"
    resp = requests.get(
        url,
        headers=_cf_headers(token),
        params={"name": name, "per_page": 100},
        timeout=30,
    )
    payload = _http_json(resp)
    return list(payload.get("result") or [])


def cf_delete_dns_record(token: str, zone_id: str, record_id: str) -> None:
    url = f"{CF_API}/zones/{zone_id}/dns_records/{record_id}"
    resp = requests.delete(url, headers=_cf_headers(token), timeout=30)
    _http_json(resp)


def cf_create_dns_record(
    token: str,
    zone_id: str,
    rtype: str,
    name: str,
    content: str,
    ttl: int = DEFAULT_TTL,
    proxied: bool = False,
) -> dict[str, Any]:
    url = f"{CF_API}/zones/{zone_id}/dns_records"
    body = {"type": rtype, "name": name, "content": content, "ttl": ttl, "proxied": proxied}
    resp = requests.post(url, headers=_cf_headers(token), data=json.dumps(body), timeout=30)
    payload = _http_json(resp)
    return dict(payload.get("result") or {})


def cf_update_dns_record(
    token: str,
    zone_id: str,
    record_id: str,
    rtype: str,
    name: str,
    content: str,
    ttl: int = DEFAULT_TTL,
    proxied: bool = False,
) -> dict[str, Any]:
    url = f"{CF_API}/zones/{zone_id}/dns_records/{record_id}"
    body = {"type": rtype, "name": name, "content": content, "ttl": ttl, "proxied": proxied}
    resp = requests.put(url, headers=_cf_headers(token), data=json.dumps(body), timeout=30)
    payload = _http_json(resp)
    return dict(payload.get("result") or {})


def _summarize_dns(records: Iterable[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for rec in records:
        out.append(
            f"{rec.get('type')} {rec.get('name')} -> {rec.get('content')} "
            f"(proxied={rec.get('proxied')}, ttl={rec.get('ttl')}) id={rec.get('id')}"
        )
    return out


def _is_conflicting_type(rec_type: str) -> bool:
    return rec_type in {"A", "AAAA", "CNAME"}


def enforce_dns_for_domain(
    cf_token: str,
    zone_id: str,
    name: str,
    desired_type: str,
    desired_content: str,
    apply: bool,
) -> dict[str, Any]:
    before = cf_list_dns_records(cf_token, zone_id, name)
    conflicts = [r for r in before if _is_conflicting_type(str(r.get("type") or ""))]
    to_delete: list[dict[str, Any]] = []

    # Delete any conflicting type that is not the desired type, and any desired-type record with wrong content.
    for rec in conflicts:
        rtype = str(rec.get("type") or "")
        content = str(rec.get("content") or "")
        if rtype != desired_type:
            to_delete.append(rec)
        elif content != desired_content:
            to_delete.append(rec)

    actions: list[str] = []
    if apply:
        for rec in to_delete:
            actions.append(f"delete {rec.get('type')} id={rec.get('id')} content={rec.get('content')}")
            cf_delete_dns_record(cf_token, zone_id, str(rec["id"]))
    else:
        for rec in to_delete:
            actions.append(f"would_delete {rec.get('type')} id={rec.get('id')} content={rec.get('content')}")

    # After deletions (or in report-only), decide whether we need create/update.
    remaining = [r for r in before if str(r.get("id")) not in {str(d.get("id")) for d in to_delete}]
    desired_existing = [
        r
        for r in remaining
        if str(r.get("type") or "") == desired_type and str(r.get("content") or "") == desired_content
    ]
    if desired_existing:
        rec = desired_existing[0]
        needs_update = bool(rec.get("proxied")) or int(rec.get("ttl") or DEFAULT_TTL) != DEFAULT_TTL
        if needs_update:
            if apply:
                actions.append(f"update {desired_type} id={rec.get('id')} set proxied=false ttl=auto")
                cf_update_dns_record(
                    cf_token,
                    zone_id,
                    str(rec["id"]),
                    desired_type,
                    name,
                    desired_content,
                    ttl=DEFAULT_TTL,
                    proxied=False,
                )
            else:
                actions.append(f"would_update {desired_type} id={rec.get('id')} set proxied=false ttl=auto")
    else:
        if apply:
            actions.append(f"create {desired_type} {name} -> {desired_content} proxied=false ttl=auto")
            cf_create_dns_record(
                cf_token,
                zone_id,
                desired_type,
                name,
                desired_content,
                ttl=DEFAULT_TTL,
                proxied=False,
            )
        else:
            actions.append(f"would_create {desired_type} {name} -> {desired_content} proxied=false ttl=auto")

    after = cf_list_dns_records(cf_token, zone_id, name) if apply else before
    return {"name": name, "before": before, "after": after, "actions": actions}


def cf_list_pagerules(token: str, zone_id: str) -> list[dict[str, Any]]:
    url = f"{CF_API}/zones/{zone_id}/pagerules"
    resp = requests.get(url, headers=_cf_headers(token), params={"per_page": 100}, timeout=30)
    payload = _http_json(resp)
    return list(payload.get("result") or [])


def cf_disable_pagerule(token: str, zone_id: str, rule_id: str) -> None:
    url = f"{CF_API}/zones/{zone_id}/pagerules/{rule_id}"
    resp = requests.patch(url, headers=_cf_headers(token), data=json.dumps({"status": "disabled"}), timeout=30)
    _http_json(resp)


def cf_list_worker_routes(token: str, zone_id: str) -> list[dict[str, Any]]:
    url = f"{CF_API}/zones/{zone_id}/workers/routes"
    resp = requests.get(url, headers=_cf_headers(token), timeout=30)
    payload = _http_json(resp)
    return list(payload.get("result") or [])


def cf_delete_worker_route(token: str, zone_id: str, route_id: str) -> None:
    url = f"{CF_API}/zones/{zone_id}/workers/routes/{route_id}"
    resp = requests.delete(url, headers=_cf_headers(token), timeout=30)
    _http_json(resp)


def cf_get_ruleset_entrypoint(token: str, zone_id: str, phase: str) -> dict[str, Any] | None:
    url = f"{CF_API}/zones/{zone_id}/rulesets/phases/{phase}/entrypoint"
    resp = requests.get(url, headers=_cf_headers(token), timeout=30)
    if resp.status_code in {400, 404}:
        return None
    return _http_json(resp).get("result")


def cf_update_ruleset_entrypoint(token: str, zone_id: str, phase: str, description: str, rules: list[dict[str, Any]]) -> None:
    url = f"{CF_API}/zones/{zone_id}/rulesets/phases/{phase}/entrypoint"
    body = {"description": description, "rules": rules}
    resp = requests.put(url, headers=_cf_headers(token), data=json.dumps(body), timeout=30)
    _http_json(resp)


def _json_contains_domain(obj: Any, domains: list[str]) -> bool:
    blob = json.dumps(obj, sort_keys=True).lower()
    return any(d.lower() in blob for d in domains)


def cleanup_redirect_sources(
    cf_token: str,
    zone_id: str,
    domains: list[str],
    apply: bool,
) -> dict[str, Any]:
    actions: list[str] = []
    findings: dict[str, Any] = {"pagerules": [], "worker_routes": [], "rulesets": []}

    # Page Rules: disable forwarding URL rules that mention the domains.
    pagerules = cf_list_pagerules(cf_token, zone_id)
    for rule in pagerules:
        if not _json_contains_domain(rule, domains):
            continue
        has_forward = any(str(a.get("id") or "") == "forwarding_url" for a in (rule.get("actions") or []))
        if not has_forward:
            continue
        findings["pagerules"].append({"id": rule.get("id"), "status": rule.get("status"), "targets": rule.get("targets"), "actions": rule.get("actions")})
        if apply and rule.get("status") != "disabled":
            actions.append(f"disable pagerule id={rule.get('id')}")
            cf_disable_pagerule(cf_token, zone_id, str(rule["id"]))
        elif not apply:
            actions.append(f"would_disable pagerule id={rule.get('id')}")

    # Workers routes: delete routes matching the domains.
    routes = cf_list_worker_routes(cf_token, zone_id)
    for route in routes:
        pattern = str(route.get("pattern") or "")
        if not any(d in pattern for d in domains):
            continue
        findings["worker_routes"].append({"id": route.get("id"), "pattern": pattern, "script": route.get("script")})
        if apply:
            actions.append(f"delete worker_route id={route.get('id')} pattern={pattern}")
            cf_delete_worker_route(cf_token, zone_id, str(route["id"]))
        else:
            actions.append(f"would_delete worker_route id={route.get('id')} pattern={pattern}")

    # Rulesets Redirect Rules (entrypoint): disable rules mentioning the domains.
    for phase in ["http_request_dynamic_redirect", "http_request_redirect"]:
        entry = cf_get_ruleset_entrypoint(cf_token, zone_id, phase)
        if not entry:
            continue
        rules = list(entry.get("rules") or [])
        changed = False
        for rule in rules:
            if not _json_contains_domain(rule, domains):
                continue
            action = str(rule.get("action") or "").lower()
            if "redirect" not in action:
                continue
            findings["rulesets"].append({"phase": phase, "id": entry.get("id"), "rule_id": rule.get("id"), "enabled": rule.get("enabled"), "expression": rule.get("expression"), "action": rule.get("action")})
            if apply and rule.get("enabled") is not False:
                rule["enabled"] = False
                changed = True
                actions.append(f"disable ruleset phase={phase} rule_id={rule.get('id')}")
            elif not apply:
                actions.append(f"would_disable ruleset phase={phase} rule_id={rule.get('id')}")
        if apply and changed:
            cf_update_ruleset_entrypoint(
                cf_token,
                zone_id,
                phase,
                str(entry.get("description") or ""),
                rules,
            )

    return {"actions": actions, "findings": findings}


def _print_section(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def main() -> int:
    parser = argparse.ArgumentParser(description="Domain Doctor: reconcile Cloudflare DNS with Vercel recommended values.")
    parser.add_argument("--project-id", default=_env("VERCEL_PROJECT_ID", DEFAULT_PROJECT_ID), help="Vercel project id")
    parser.add_argument("--apex", default=_env("DOMAIN_APEX", DEFAULT_APEX), help="Apex domain")
    parser.add_argument("--www", default=_env("DOMAIN_WWW", DEFAULT_WWW), help="WWW domain")
    parser.add_argument("--apply-dns", action="store_true", help="Apply Cloudflare DNS changes (default is report-only)")
    parser.add_argument("--apply-redirect-cleanup", action="store_true", help="Optionally disable/delete redirect sources in Cloudflare")
    parser.add_argument("--json-report", default="", help="Optional path to write JSON report")
    args = parser.parse_args()

    vercel_token = _env("VERCEL_TOKEN", required=True)
    team_id = _env("VERCEL_TEAM_ID", default="")
    cf_token = _env("CLOUDFLARE_API_TOKEN", required=True)
    cf_zone_id = _env("CF_ZONE_ID", default="")
    cf_zone_name = _env("CF_ZONE_NAME", default=args.apex)

    report: dict[str, Any] = {"timestamp_utc": _now_utc(), "inputs": vars(args), "actions": []}

    _print_section("Vercel Project")
    project = vercel_get_project(vercel_token, args.project_id, team_id=team_id)
    print(f"project_id={args.project_id}")
    print(f"name={project.get('name')}")
    report["vercel_project"] = {"id": args.project_id, "name": project.get("name")}

    domains = vercel_list_project_domains(vercel_token, args.project_id, team_id=team_id)
    attached = sorted([str(d.get("name") or "") for d in domains if d.get("name")])
    print(f"attached_domains={', '.join(attached) if attached else '(none)'}")
    report["vercel_attached_domains"] = attached

    _print_section("Vercel Recommended DNS")
    rec = get_vercel_recommendations(vercel_token, args.project_id, args.apex, args.www, team_id=team_id)
    if not rec.apex_a:
        raise DomainDoctorError(f"Vercel did not return recommendedIPv4 for apex {args.apex}")
    if not rec.www_cname:
        raise DomainDoctorError(f"Vercel did not return recommendedCNAME for www {args.www}")
    print(f"apex A target={rec.apex_a}")
    print(f"www CNAME target={rec.www_cname}")
    report["vercel_recommended"] = {"apex_a": rec.apex_a, "www_cname": rec.www_cname}

    # Validation status (best-effort).
    for d in [args.apex, args.www]:
        dom = vercel_get_project_domain(vercel_token, args.project_id, d, team_id=team_id)
        if not dom:
            print(f"domain_status {d}: not attached to project (API 404)")
            continue
        verified = dom.get("verified")
        validation = dom.get("validation") or []
        print(f"domain_status {d}: verified={verified} validation_count={len(validation)}")

    _print_section("Cloudflare Zone")
    zone_id = cf_get_zone_id(cf_token, cf_zone_id, cf_zone_name)
    print(f"zone_id={zone_id} zone_name={cf_zone_name}")
    report["cloudflare_zone"] = {"id": zone_id, "name": cf_zone_name}

    _print_section("Cloudflare DNS Plan")
    apex_result = enforce_dns_for_domain(cf_token, zone_id, args.apex, "A", rec.apex_a, apply=args.apply_dns)
    www_result = enforce_dns_for_domain(cf_token, zone_id, args.www, "CNAME", rec.www_cname, apply=args.apply_dns)
    report["dns"] = {"apex": apex_result, "www": www_result}

    print("apex_before:")
    for line in _summarize_dns(apex_result["before"]):
        print(f"  {line}")
    print("apex_actions:")
    for line in apex_result["actions"]:
        print(f"  {line}")

    print("www_before:")
    for line in _summarize_dns(www_result["before"]):
        print(f"  {line}")
    print("www_actions:")
    for line in www_result["actions"]:
        print(f"  {line}")

    if args.apply_redirect_cleanup:
        _print_section("Cloudflare Redirect Cleanup (Apply)")
    else:
        _print_section("Cloudflare Redirect Cleanup (Report-Only)")
    redirects = cleanup_redirect_sources(
        cf_token,
        zone_id,
        domains=[args.apex, args.www],
        apply=bool(args.apply_redirect_cleanup),
    )
    report["redirects"] = redirects
    if redirects["findings"]["pagerules"] or redirects["findings"]["worker_routes"] or redirects["findings"]["rulesets"]:
        print("findings:")
        print(json.dumps(redirects["findings"], indent=2))
    else:
        print("findings: none")
    print("actions:")
    for a in redirects["actions"]:
        print(f"  {a}")

    _print_section("Vercel Re-check")
    rec2 = get_vercel_recommendations(vercel_token, args.project_id, args.apex, args.www, team_id=team_id)
    apex_mis = bool(rec2.apex_raw.get("misconfigured"))
    www_mis = bool(rec2.www_raw.get("misconfigured"))
    print(f"apex misconfigured={apex_mis}")
    print(f"www misconfigured={www_mis}")

    _print_section("Verification Commands")
    print("DNS:")
    print(f"  Resolve-DnsName {args.apex} -Type A")
    print(f"  Resolve-DnsName {args.www} -Type CNAME")
    print("HTTP:")
    print(f"  curl -I https://{args.apex}/")
    print(f"  curl -I https://{args.www}/")

    if args.json_report:
        out_path = os.path.abspath(args.json_report)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
            f.write("\n")
        print(f"\nreport_json={out_path}")

    if apex_mis or www_mis:
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DomainDoctorError as exc:
        print(f"DOMAIN_DOCTOR_ERROR {exc}", file=sys.stderr)
        raise SystemExit(1)

