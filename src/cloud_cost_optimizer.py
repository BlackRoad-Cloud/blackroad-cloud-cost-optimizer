"""BlackRoad Cloud Cost Optimizer - cloud cost analysis and right-sizing recommendations."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

GREEN = "\033[0;32m"
RED   = "\033[0;31m"
YELLOW= "\033[1;33m"
CYAN  = "\033[0;36m"
BLUE  = "\033[0;34m"
BOLD  = "\033[1m"
NC    = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "cloud-cost.db"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class CloudService:
    id: Optional[int]
    name: str
    provider: str          # aws | gcp | azure | digitalocean
    service_type: str      # compute | storage | database | network
    region: str
    monthly_cost: float
    allocated_vcpu: float
    allocated_ram_gb: float
    avg_cpu_pct: float
    avg_mem_pct: float
    status: str            # running | stopped | rightsizing
    tags: str              # JSON string
    created_at: Optional[str] = None


@dataclass
class CostRecommendation:
    id: Optional[int]
    service_id: int
    rec_type: str          # downsize | rightsize | terminate | reserved
    description: str
    estimated_savings: float
    confidence: float      # 0.0 – 1.0
    status: str            # pending | applied | dismissed
    created_at: Optional[str] = None


# ---------------------------------------------------------------------------
# Core business logic
# ---------------------------------------------------------------------------

class CloudCostOptimizer:
    """Analyse cloud resource utilisation and produce right-sizing recommendations."""

    IDLE_CPU    = 10.0    # % below which a service is considered idle
    IDLE_MEM    = 15.0
    UNDER_CPU   = 30.0    # % below which downsize is recommended
    SPIKE_MULTI = 2.5     # multiplier for reserved-instance opportunity
    DOWNSIZE_SAVING = 0.40

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS cloud_services (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    name             TEXT NOT NULL,
                    provider         TEXT NOT NULL,
                    service_type     TEXT NOT NULL,
                    region           TEXT NOT NULL DEFAULT 'us-east-1',
                    monthly_cost     REAL NOT NULL DEFAULT 0.0,
                    allocated_vcpu   REAL NOT NULL DEFAULT 1.0,
                    allocated_ram_gb REAL NOT NULL DEFAULT 1.0,
                    avg_cpu_pct      REAL NOT NULL DEFAULT 0.0,
                    avg_mem_pct      REAL NOT NULL DEFAULT 0.0,
                    status           TEXT NOT NULL DEFAULT 'running',
                    tags             TEXT DEFAULT '{}',
                    created_at       TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS recommendations (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    service_id        INTEGER REFERENCES cloud_services(id),
                    rec_type          TEXT NOT NULL,
                    description       TEXT NOT NULL,
                    estimated_savings REAL NOT NULL DEFAULT 0.0,
                    confidence        REAL NOT NULL DEFAULT 0.5,
                    status            TEXT NOT NULL DEFAULT 'pending',
                    created_at        TEXT DEFAULT CURRENT_TIMESTAMP
                );
            """)

    def add_service(self, name: str, provider: str, service_type: str,
                    region: str, monthly_cost: float, allocated_vcpu: float,
                    allocated_ram_gb: float, avg_cpu_pct: float = 0.0,
                    avg_mem_pct: float = 0.0, tags: Optional[dict] = None) -> CloudService:
        """Register a cloud resource for cost tracking."""
        tags_str = json.dumps(tags or {})
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """INSERT INTO cloud_services
                   (name, provider, service_type, region, monthly_cost,
                    allocated_vcpu, allocated_ram_gb, avg_cpu_pct, avg_mem_pct, tags)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (name, provider, service_type, region, monthly_cost,
                 allocated_vcpu, allocated_ram_gb, avg_cpu_pct, avg_mem_pct, tags_str),
            )
            conn.commit()
        return self._get_service(cur.lastrowid)

    def _get_service(self, service_id: int) -> Optional[CloudService]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM cloud_services WHERE id = ?", (service_id,)
            ).fetchone()
        return CloudService(**dict(row)) if row else None

    def list_services(self, provider: Optional[str] = None,
                      service_type: Optional[str] = None) -> list[CloudService]:
        """Retrieve all tracked cloud services."""
        q, params = "SELECT * FROM cloud_services WHERE 1=1", []
        if provider:
            q += " AND provider = ?";      params.append(provider)
        if service_type:
            q += " AND service_type = ?";  params.append(service_type)
        q += " ORDER BY monthly_cost DESC"
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(q, params).fetchall()
        return [CloudService(**dict(r)) for r in rows]

    def analyze_and_recommend(self) -> list[CostRecommendation]:
        """Analyse utilisation data and produce right-sizing recommendations."""
        services = self.list_services()
        recs: list[CostRecommendation] = []
        for svc in services:
            if (svc.avg_cpu_pct < self.IDLE_CPU and svc.avg_mem_pct < self.IDLE_MEM
                    and svc.monthly_cost > 5.0):
                recs.append(self._create_rec(
                    svc.id, "terminate",
                    f"Idle (CPU {svc.avg_cpu_pct:.1f}%, Mem {svc.avg_mem_pct:.1f}%). "
                    f"Consider termination.",
                    round(svc.monthly_cost * 0.90, 2), 0.85
                ))
            elif (svc.avg_cpu_pct < self.UNDER_CPU and svc.allocated_vcpu > 2
                  and svc.monthly_cost > 20.0):
                recs.append(self._create_rec(
                    svc.id, "downsize",
                    f"CPU utilisation low ({svc.avg_cpu_pct:.1f}%). "
                    f"Downsize from {svc.allocated_vcpu} vCPU.",
                    round(svc.monthly_cost * self.DOWNSIZE_SAVING, 2), 0.75
                ))
            elif svc.status == "running" and svc.monthly_cost > 50.0:
                saving = round(svc.monthly_cost * 0.30, 2)
                recs.append(self._create_rec(
                    svc.id, "reserved",
                    f"Long-running: reserved/committed-use discount could save "
                    f"~${saving}/mo (≈30%).",
                    saving, 0.70
                ))
        return recs

    def _create_rec(self, service_id: int, rec_type: str, description: str,
                    savings: float, confidence: float) -> CostRecommendation:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """INSERT INTO recommendations
                   (service_id, rec_type, description, estimated_savings, confidence)
                   VALUES (?, ?, ?, ?, ?)""",
                (service_id, rec_type, description, savings, confidence),
            )
            conn.commit()
        return CostRecommendation(id=cur.lastrowid, service_id=service_id,
                                  rec_type=rec_type, description=description,
                                  estimated_savings=savings, confidence=confidence,
                                  status="pending")

    def cost_status(self) -> dict:
        """Aggregate cost summary across all providers."""
        services = self.list_services()
        total = sum(s.monthly_cost for s in services)
        by_provider: dict[str, float] = {}
        by_type:     dict[str, float] = {}
        for s in services:
            by_provider[s.provider]     = by_provider.get(s.provider, 0.0) + s.monthly_cost
            by_type[s.service_type]     = by_type.get(s.service_type, 0.0) + s.monthly_cost
        with sqlite3.connect(self.db_path) as conn:
            pending_savings = conn.execute(
                "SELECT COALESCE(SUM(estimated_savings),0) FROM recommendations "
                "WHERE status='pending'"
            ).fetchone()[0]
        return {
            "total_monthly":    round(total, 2),
            "annual_projection": round(total * 12, 2),
            "service_count":    len(services),
            "pending_savings":  round(pending_savings, 2),
            "by_provider": {k: round(v, 2) for k, v in sorted(by_provider.items())},
            "by_type":     {k: round(v, 2) for k, v in
                            sorted(by_type.items(), key=lambda x: -x[1])},
        }

    def export_json(self, output_path: str = "cloud_cost_export.json") -> str:
        """Export all cost data and recommendations to JSON."""
        services = self.list_services()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            recs = [dict(r) for r in conn.execute(
                "SELECT * FROM recommendations ORDER BY estimated_savings DESC"
            ).fetchall()]
        payload = {
            "exported_at": datetime.now().isoformat(),
            "summary":     self.cost_status(),
            "services":    [asdict(s) for s in services],
            "recommendations": recs,
        }
        with open(output_path, "w") as fh:
            json.dump(payload, fh, indent=2)
        return output_path


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _util_bar(pct: float, width: int = 10) -> str:
    filled = int(min(pct, 100) / 100 * width)
    color  = GREEN if pct < 50 else (YELLOW if pct < 80 else RED)
    return f"{color}{'█' * filled}{'░' * (width - filled)}{NC} {pct:5.1f}%"


def _print_service(s: CloudService) -> None:
    sc = GREEN if s.status == "running" else YELLOW
    print(f"  {BOLD}[{s.id:>3}]{NC} {CYAN}{s.name}{NC}  {BLUE}({s.provider} · {s.service_type}){NC}")
    print(f"        Status : {sc}{s.status}{NC}   Region: {s.region}")
    print(f"        Cost   : {YELLOW}${s.monthly_cost:>8,.2f}/mo{NC}   "
          f"vCPU: {s.allocated_vcpu}   RAM: {s.allocated_ram_gb} GB")
    print(f"        CPU    : {_util_bar(s.avg_cpu_pct)}")
    print(f"        Memory : {_util_bar(s.avg_mem_pct)}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cloud_cost_optimizer",
        description="BlackRoad Cloud Cost Optimizer — right-sizing and cost analysis",
    )
    sub = p.add_subparsers(dest="cmd", metavar="COMMAND")

    lp = sub.add_parser("list", help="List cloud services")
    lp.add_argument("--provider",     default=None)
    lp.add_argument("--type",         dest="service_type", default=None)

    ap = sub.add_parser("add", help="Register a cloud service")
    ap.add_argument("name")
    ap.add_argument("provider",     choices=["aws", "gcp", "azure", "digitalocean", "other"])
    ap.add_argument("service_type", choices=["compute", "storage", "database", "network", "other"])
    ap.add_argument("region")
    ap.add_argument("monthly_cost", type=float)
    ap.add_argument("--vcpu",    type=float, default=1.0,  dest="allocated_vcpu")
    ap.add_argument("--ram",     type=float, default=1.0,  dest="allocated_ram_gb")
    ap.add_argument("--cpu-pct", type=float, default=0.0,  dest="avg_cpu_pct")
    ap.add_argument("--mem-pct", type=float, default=0.0,  dest="avg_mem_pct")

    sub.add_parser("analyze", help="Generate cost optimisation recommendations")
    sub.add_parser("status",  help="Show cost dashboard summary")

    ep = sub.add_parser("export", help="Export cost data to JSON")
    ep.add_argument("--output", default="cloud_cost_export.json")

    return p


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()
    opt    = CloudCostOptimizer()
    print(f"\n{BOLD}{BLUE}╔══ BlackRoad Cloud Cost Optimizer ══╗{NC}\n")

    if args.cmd == "list":
        svcs = opt.list_services(provider=getattr(args, "provider", None),
                                 service_type=getattr(args, "service_type", None))
        if not svcs:
            print(f"  {YELLOW}No services registered.{NC}\n"); return
        total = sum(s.monthly_cost for s in svcs)
        print(f"  {BOLD}Cloud Services ({len(svcs)}) — Total: {YELLOW}${total:,.2f}/mo{NC}\n")
        for s in svcs:
            _print_service(s)

    elif args.cmd == "add":
        svc = opt.add_service(args.name, args.provider, args.service_type, args.region,
                              args.monthly_cost, args.allocated_vcpu, args.allocated_ram_gb,
                              args.avg_cpu_pct, args.avg_mem_pct)
        print(f"  {GREEN}✓ Service registered: [{svc.id}] {svc.name} "
              f"@ ${svc.monthly_cost:,.2f}/mo{NC}\n")

    elif args.cmd == "analyze":
        recs = opt.analyze_and_recommend()
        if not recs:
            print(f"  {GREEN}✓ No optimisation opportunities found.{NC}\n"); return
        total_sav = sum(r.estimated_savings for r in recs)
        print(f"  {BOLD}{YELLOW}⚡ {len(recs)} Recommendations — "
              f"Est. savings: ${total_sav:,.2f}/mo{NC}\n")
        for r in recs:
            tc  = RED if r.rec_type == "terminate" else YELLOW
            bar = "●" * int(r.confidence * 5) + "○" * (5 - int(r.confidence * 5))
            print(f"  {BOLD}[{r.id:>3}]{NC} {tc}[{r.rec_type.upper()}]{NC}  "
                  f"svc #{r.service_id}   confidence {CYAN}{bar}{NC}")
            print(f"        {r.description}")
            print(f"        {GREEN}Est. savings: ${r.estimated_savings:,.2f}/mo{NC}\n")

    elif args.cmd == "status":
        s = opt.cost_status()
        print(f"  {BOLD}Cloud Cost Dashboard{NC}")
        print(f"  {'Monthly Total':<26} {YELLOW}${s['total_monthly']:>10,.2f}{NC}")
        print(f"  {'Annual Projection':<26} ${s['annual_projection']:>10,.2f}")
        print(f"  {'Services Tracked':<26} {s['service_count']}")
        print(f"  {'Pending Savings':<26} {GREEN}${s['pending_savings']:>10,.2f}/mo{NC}")
        if s["by_provider"]:
            print(f"\n  {BOLD}By Provider:{NC}")
            for prov, cost in s["by_provider"].items():
                print(f"    {CYAN}{prov:<18}{NC} ${cost:>10,.2f}/mo")
        if s["by_type"]:
            print(f"\n  {BOLD}By Service Type:{NC}")
            for stype, cost in s["by_type"].items():
                print(f"    {CYAN}{stype:<18}{NC} ${cost:>10,.2f}/mo")
        print()

    elif args.cmd == "export":
        path = opt.export_json(args.output)
        print(f"  {GREEN}✓ Exported to: {path}{NC}\n")

    else:
        parser.print_help(); print()


if __name__ == "__main__":
    main()
