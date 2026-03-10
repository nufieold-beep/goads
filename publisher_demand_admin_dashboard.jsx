import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  LayoutDashboard,
  ChevronDown,
  ChevronRight,
  Radio,
  Building2,
  Boxes,
  Package2,
  ShoppingCart,
  HandCoins,
  GitBranch,
  Search,
  Plus,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

// ── Backend API response shapes (JSON from /dashboard/supply-partners etc.) ────
type SupplyPartnerAPI = {
  id: string;
  name: string;
  delivery_status: "Live" | "Limited" | "Paused" | "Archived";
  active: boolean;
  opportunities: number;
  gross_revenue: number;
  avg_qps_yesterday: number;
  avg_qps_last_hour: number;
  impressions: number;
  publisher_payout: number;
  completions: number;
  viewable_impressions: number;
};

type DemandPartnerAPI = {
  id: string;
  name: string;
  delivery_status: "Live" | "Limited" | "Paused" | "Archived";
  active: boolean;
  bid_requests: number;
  bids: number;
  avg_qps_yesterday: number;
  avg_qps_last_hour: number;
  impressions: number;
  gross_revenue: number;
  payout: number;
  completions: number;
  viewable_impressions: number;
};

// ── Modal form state ─────────────────────────────────────────────────────────
type ModalState = {
  tab: "supply" | "demand";
  mode: "create" | "edit";
  id?: string;
  name: string;
  deliveryStatus: string;
  active: boolean;
};

type SupplyRow = {
  enabled: boolean;
  id: string;
  name: string;
  deliveryStatus: "Live" | "Limited" | "Paused" | "Archived";
  opportunities: number;
  grossRevenue: number;
  opportunityEcpm: number;
  opportunityFillRate: number;
  avgQpsYesterday: number;
  avgQpsLastHour: number;
  impressions: number;
  publisherPayout: number;
  /** Video completions — VCR = completions / impressions × 100 */
  completions: number;
  /** Viewable impressions — Viewability = viewableImpressions / impressions × 100 */
  viewableImpressions: number;
};

type DemandRow = {
  enabled: boolean;
  id: string;
  name: string;
  deliveryStatus: "Live" | "Limited" | "Paused" | "Archived";
  bidRequests: number;
  bidRequestFillRate: number;
  /** Revenue per 1 000 bid requests — mirrors Opportunity eCPM on the supply side */
  bidRequestEcpm: number;
  bids: number;
  avgQpsYesterday: number;
  avgQpsLastHour: number;
  impressions: number;
  grossRevenue: number;
  /** Revenue per 1 000 impressions won */
  winEcpm: number;
  payout: number;
  /** Video completions — VCR = completions / impressions × 100 */
  completions: number;
  /** Viewable impressions — Viewability = viewableImpressions / impressions × 100 */
  viewableImpressions: number;
  actions: string;
};

type BidReportAPI = {
  id: string;
  request_id: string;
  imp_id: string;
  publisher_id: string;
  ad_unit_id: string;
  bidder: string;
  adomain: string[];
  crid: string;
  campaign_id: string;
  deal_id: string;
  cid: string;
  burl: string;
  price: number;
  currency: string;
  event_type: string;
  event_time: string;
  env: string;
  app_bundle: string;
  domain: string;
  country_code: string;
  error_code: number;
  error_msg: string;
  created_at: string;
};



// ── API → view-model mappers ───────────────────────────────────────────────────
function mapSupplyPartner(a: SupplyPartnerAPI): SupplyRow {
  return {
    enabled: a.active,
    id: a.id,
    name: a.name,
    deliveryStatus: a.delivery_status,
    opportunities: a.opportunities,
    grossRevenue: a.gross_revenue,
    opportunityEcpm: a.opportunities > 0 ? (a.gross_revenue / a.opportunities) * 1000 : 0,
    opportunityFillRate: a.opportunities > 0 ? (a.impressions / a.opportunities) * 100 : 0,
    avgQpsYesterday: a.avg_qps_yesterday,
    avgQpsLastHour: a.avg_qps_last_hour,
    impressions: a.impressions,
    publisherPayout: a.publisher_payout,
    completions: a.completions,
    viewableImpressions: a.viewable_impressions,
  };
}

function mapDemandPartner(a: DemandPartnerAPI): DemandRow {
  return {
    enabled: a.active,
    id: a.id,
    name: a.name,
    deliveryStatus: a.delivery_status,
    bidRequests: a.bid_requests,
    bidRequestFillRate: a.bid_requests > 0 ? (a.bids / a.bid_requests) * 100 : 0,
    bidRequestEcpm: a.bid_requests > 0 ? (a.gross_revenue / a.bid_requests) * 1000 : 0,
    bids: a.bids,
    avgQpsYesterday: a.avg_qps_yesterday,
    avgQpsLastHour: a.avg_qps_last_hour,
    impressions: a.impressions,
    grossRevenue: a.gross_revenue,
    winEcpm: a.impressions > 0 ? (a.gross_revenue / a.impressions) * 1000 : 0,
    payout: a.payout,
    completions: a.completions,
    viewableImpressions: a.viewable_impressions,
    actions: "Edit",
  };
}

function formatInteger(value: number) {
  return new Intl.NumberFormat().format(value);
}

function formatCurrency(value: number) {
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatPercent(value: number) {
  return `${value.toFixed(1)}%`;
}

function formatQps(value: number) {
  return value.toLocaleString();
}

function StatusBadge({ status }: { status: SupplyRow["deliveryStatus"] | DemandRow["deliveryStatus"] }) {
  const variant =
    status === "Live"
      ? "default"
      : status === "Limited"
        ? "secondary"
        : status === "Paused"
          ? "outline"
          : "secondary";
  return <Badge variant={variant as any} className="rounded-xl">{status}</Badge>;
}

function SidebarSection({
  icon: Icon,
  title,
  items,
  defaultOpen = true,
}: {
  icon: any;
  title: string;
  items: string[];
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className="space-y-1">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center justify-between rounded-2xl px-3 py-2 text-left hover:bg-muted"
      >
        <span className="flex items-center gap-3 text-sm font-medium">
          <Icon className="h-4 w-4" />
          {title}
        </span>
        {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
      </button>
      {open ? (
        <div className="ml-4 space-y-1 border-l pl-3">
          {items.map((item) => (
            <button key={item} className="block w-full rounded-xl px-3 py-2 text-left text-sm text-muted-foreground hover:bg-muted hover:text-foreground">
              {item}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function FiltersBar({
  filterText,
  setFilterText,
  status,
  setStatus,
  showArchived,
  setShowArchived,
  deliveryStatus,
  setDeliveryStatus,
  deliveryGroup,
  setDeliveryGroup,
  onCreateNew,
}: {
  filterText: string;
  setFilterText: (value: string) => void;
  status: string;
  setStatus: (value: string) => void;
  showArchived: boolean;
  setShowArchived: (value: boolean) => void;
  deliveryStatus: string;
  setDeliveryStatus: (value: string) => void;
  deliveryGroup: string;
  setDeliveryGroup: (value: string) => void;
  onCreateNew: () => void;
}) {
  return (
    <Card className="rounded-[28px] shadow-sm">
      <CardContent className="p-4">
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[180px_1fr_180px_180px_180px_220px_220px]">
          <Button className="rounded-2xl bg-red-600 hover:bg-red-700" onClick={onCreateNew}>
            <Plus className="mr-2 h-4 w-4" />
            Create new
          </Button>

          <div className="relative">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              placeholder="Filter"
              className="rounded-2xl pl-9"
            />
          </div>

          <Select value={status} onValueChange={setStatus}>
            <SelectTrigger className="rounded-2xl">
              <SelectValue placeholder="Status" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Status: All</SelectItem>
              <SelectItem value="on">On</SelectItem>
              <SelectItem value="off">Off</SelectItem>
            </SelectContent>
          </Select>

          <div className="flex items-center gap-3 rounded-2xl border px-4 py-2">
            <Checkbox checked={showArchived} onCheckedChange={(v) => setShowArchived(Boolean(v))} id="archived" />
            <Label htmlFor="archived" className="cursor-pointer">Show archived</Label>
          </div>

          <Input type="text" className="rounded-2xl" defaultValue="Last 7 days" />

          <Select value={deliveryStatus} onValueChange={setDeliveryStatus}>
            <SelectTrigger className="rounded-2xl">
              <SelectValue placeholder="Delivery Status" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Delivery Status: All</SelectItem>
              <SelectItem value="live">Live</SelectItem>
              <SelectItem value="limited">Limited</SelectItem>
              <SelectItem value="paused">Paused</SelectItem>
              <SelectItem value="archived">Archived</SelectItem>
            </SelectContent>
          </Select>

          <Select value={deliveryGroup} onValueChange={setDeliveryGroup}>
            <SelectTrigger className="rounded-2xl">
              <SelectValue placeholder="Delivery Status Group" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Delivery Status Group: All</SelectItem>
              <SelectItem value="healthy">Healthy</SelectItem>
              <SelectItem value="warning">Warning</SelectItem>
              <SelectItem value="inactive">Inactive</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </CardContent>
    </Card>
  );
}

export default function PublisherDemandAdminDashboard() {
  // ── Filtering ────────────────────────────────────────────────────────────────
  const [filterText, setFilterText] = useState("");
  const [status, setStatus] = useState("all");
  const [showArchived, setShowArchived] = useState(false);
  const [deliveryStatus, setDeliveryStatus] = useState("all");
  const [deliveryGroup, setDeliveryGroup] = useState("all");
  const [activeTab, setActiveTab] = useState<"supply" | "demand" | "reports">("supply");

  // ── Server data ───────────────────────────────────────────────────────────────
  const [supplyApi, setSupplyApi] = useState<SupplyPartnerAPI[]>([]);
  const [demandApi, setDemandApi] = useState<DemandPartnerAPI[]>([]);
  const [reports, setReports] = useState<BidReportAPI[]>([]);
  const [reportFilters, setReportFilters] = useState({ campaign_id: "", crid: "", adomain: "", bidder: "", event_type: "", publisher_id: "" });
  const [reportLoading, setReportLoading] = useState(false);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState<string | null>(null);

  // ── Modal ─────────────────────────────────────────────────────────────────────
  const [modal, setModal] = useState<ModalState | null>(null);
  const [saving, setSaving] = useState(false);

  // ── Fetch ─────────────────────────────────────────────────────────────────────
  const fetchData = useCallback(async () => {
    setLoading(true);
    setFetchError(null);
    try {
      const [sp, dp] = await Promise.all([
        fetch("/dashboard/supply-partners").then((r) => r.json()),
        fetch("/dashboard/demand-partners").then((r) => r.json()),
      ]);
      setSupplyApi(Array.isArray(sp.entries) ? sp.entries : []);
      setDemandApi(Array.isArray(dp.entries) ? dp.entries : []);
    } catch (err: any) {
      setFetchError(err?.message ?? "Failed to load dashboard data");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void fetchData();
  }, [fetchData]);

  // ── Fetch reports ─────────────────────────────────────────────────────────────
  const fetchReports = useCallback(async () => {
    setReportLoading(true);
    try {
      const params = new URLSearchParams();
      if (reportFilters.campaign_id) params.set("campaign_id", reportFilters.campaign_id);
      if (reportFilters.crid) params.set("crid", reportFilters.crid);
      if (reportFilters.adomain) params.set("adomain", reportFilters.adomain);
      if (reportFilters.bidder) params.set("bidder", reportFilters.bidder);
      if (reportFilters.event_type) params.set("event_type", reportFilters.event_type);
      if (reportFilters.publisher_id) params.set("publisher_id", reportFilters.publisher_id);
      const r = await fetch(`/dashboard/reports?${params.toString()}`).then((res) => res.json());
      setReports(Array.isArray(r.entries) ? r.entries : []);
    } catch {
      setReports([]);
    } finally {
      setReportLoading(false);
    }
  }, [reportFilters]);

  useEffect(() => {
    if (activeTab === "reports") void fetchReports();
  }, [activeTab, fetchReports]);

  // ── Mapped display rows ───────────────────────────────────────────────────────
  const supplyData = useMemo(() => supplyApi.map(mapSupplyPartner), [supplyApi]);
  const demandData = useMemo(() => demandApi.map(mapDemandPartner), [demandApi]);

  // ── Toggle active (optimistic update) ────────────────────────────────────────
  const handleToggle = useCallback(
    async (tab: "supply" | "demand", id: string, active: boolean) => {
      if (tab === "supply") {
        setSupplyApi((prev) => prev.map((p) => (p.id === id ? { ...p, active } : p)));
      } else {
        setDemandApi((prev) => prev.map((p) => (p.id === id ? { ...p, active } : p)));
      }
      try {
        const path =
          tab === "supply"
            ? `/dashboard/supply-partners/${id}`
            : `/dashboard/demand-partners/${id}`;
        await fetch(path, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ active }),
        });
      } catch {
        void fetchData(); // revert optimistic update on network error
      }
    },
    [fetchData],
  );

  // ── Modal helpers ─────────────────────────────────────────────────────────────
  const openCreate = useCallback(
    (tab: "supply" | "demand") =>
      setModal({ tab, mode: "create", name: "", deliveryStatus: "Live", active: true }),
    [],
  );

  const openEdit = useCallback(
    (tab: "supply" | "demand", row: { id: string; name: string; delivery_status: string; active: boolean }) =>
      setModal({ tab, mode: "edit", id: row.id, name: row.name, deliveryStatus: row.delivery_status, active: row.active }),
    [],
  );

  const handleModalSubmit = useCallback(async () => {
    if (!modal || !modal.name.trim()) return;
    setSaving(true);
    try {
      const payload = { name: modal.name, delivery_status: modal.deliveryStatus, active: modal.active };
      if (modal.mode === "create") {
        const path = modal.tab === "supply" ? "/dashboard/supply-partners" : "/dashboard/demand-partners";
        const res = await fetch(path, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (modal.tab === "supply") {
          const created: SupplyPartnerAPI = await res.json();
          setSupplyApi((prev) => [...prev, created]);
        } else {
          const created: DemandPartnerAPI = await res.json();
          setDemandApi((prev) => [...prev, created]);
        }
      } else {
        const id = modal.id!;
        if (modal.tab === "supply") {
          const existing = supplyApi.find((p) => p.id === id) ?? ({} as SupplyPartnerAPI);
          const res = await fetch(`/dashboard/supply-partners/${id}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ...existing, ...payload }),
          });
          const updated: SupplyPartnerAPI = await res.json();
          setSupplyApi((prev) => prev.map((p) => (p.id === id ? updated : p)));
        } else {
          const existing = demandApi.find((p) => p.id === id) ?? ({} as DemandPartnerAPI);
          const res = await fetch(`/dashboard/demand-partners/${id}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ...existing, ...payload }),
          });
          const updated: DemandPartnerAPI = await res.json();
          setDemandApi((prev) => prev.map((p) => (p.id === id ? updated : p)));
        }
      }
      setModal(null);
    } finally {
      setSaving(false);
    }
  }, [modal, supplyApi, demandApi]);

  // ── Totals ─────────────────────────────────────────────────────────────────────
  const supplyTotals = useMemo(() => {
    const rows = supplyData;
    const totalOpportunities = rows.reduce((s, r) => s + r.opportunities, 0);
    const totalImpressions = rows.reduce((s, r) => s + r.impressions, 0);
    const totalGrossRevenue = rows.reduce((s, r) => s + r.grossRevenue, 0);
    const totalPublisherPayout = rows.reduce((s, r) => s + r.publisherPayout, 0);
    const totalCompletions = rows.reduce((s, r) => s + r.completions, 0);
    const totalViewableImpressions = rows.reduce((s, r) => s + r.viewableImpressions, 0);
    return {
      totalOpportunities,
      totalImpressions,
      totalGrossRevenue,
      totalPublisherPayout,
      // opportunityEcpm = totalGrossRevenue / totalOpportunities * 1000
      opportunityEcpm: totalOpportunities > 0 ? (totalGrossRevenue / totalOpportunities) * 1000 : 0,
      // fillRate = totalImpressions / totalOpportunities * 100
      fillRate: totalOpportunities > 0 ? (totalImpressions / totalOpportunities) * 100 : 0,
      // VCR (impression-weighted) = totalCompletions / totalImpressions * 100
      vcr: totalImpressions > 0 ? (totalCompletions / totalImpressions) * 100 : 0,
      // Viewability (impression-weighted) = totalViewableImpressions / totalImpressions * 100
      viewability: totalImpressions > 0 ? (totalViewableImpressions / totalImpressions) * 100 : 0,
    };
  }, [supplyData]);

  const demandTotals = useMemo(() => {
    const rows = demandData;
    const totalBidRequests = rows.reduce((s, r) => s + r.bidRequests, 0);
    const totalBids = rows.reduce((s, r) => s + r.bids, 0);
    const totalImpressions = rows.reduce((s, r) => s + r.impressions, 0);
    const totalGrossRevenue = rows.reduce((s, r) => s + r.grossRevenue, 0);
    const totalPayout = rows.reduce((s, r) => s + r.payout, 0);
    const totalCompletions = rows.reduce((s, r) => s + r.completions, 0);
    const totalViewableImpressions = rows.reduce((s, r) => s + r.viewableImpressions, 0);
    return {
      totalBidRequests,
      totalBids,
      totalImpressions,
      totalGrossRevenue,
      totalPayout,
      // bidRequestEcpm = totalGrossRevenue / totalBidRequests * 1000
      bidRequestEcpm: totalBidRequests > 0 ? (totalGrossRevenue / totalBidRequests) * 1000 : 0,
      // winEcpm = totalGrossRevenue / totalImpressions * 1000
      winEcpm: totalImpressions > 0 ? (totalGrossRevenue / totalImpressions) * 1000 : 0,
      // bidRequestFillRate = totalBids / totalBidRequests * 100
      bidRequestFillRate: totalBidRequests > 0 ? (totalBids / totalBidRequests) * 100 : 0,
      // VCR (impression-weighted) = totalCompletions / totalImpressions * 100
      vcr: totalImpressions > 0 ? (totalCompletions / totalImpressions) * 100 : 0,
      // Viewability (impression-weighted) = totalViewableImpressions / totalImpressions * 100
      viewability: totalImpressions > 0 ? (totalViewableImpressions / totalImpressions) * 100 : 0,
    };
  }, [demandData]);

  const filteredSupply = useMemo(() => {
    return supplyData.filter((row) => {
      const q = filterText.toLowerCase();
      const matchesText = !q || row.id.toLowerCase().includes(q) || row.name.toLowerCase().includes(q);
      const matchesStatus = status === "all" || (status === "on" ? row.enabled : !row.enabled);
      const matchesDelivery = deliveryStatus === "all" || row.deliveryStatus.toLowerCase() === deliveryStatus;
      const matchesArchived = showArchived || row.deliveryStatus !== "Archived";
      const matchesGroup =
        deliveryGroup === "all" ||
        (deliveryGroup === "healthy" && row.deliveryStatus === "Live") ||
        (deliveryGroup === "warning" && row.deliveryStatus === "Limited") ||
        (deliveryGroup === "inactive" && (row.deliveryStatus === "Paused" || row.deliveryStatus === "Archived"));

      return matchesText && matchesStatus && matchesDelivery && matchesArchived && matchesGroup;
    });
  }, [supplyData, filterText, status, deliveryStatus, showArchived, deliveryGroup]);

  const filteredDemand = useMemo(() => {
    return demandData.filter((row) => {
      const q = filterText.toLowerCase();
      const matchesText = !q || row.id.toLowerCase().includes(q) || row.name.toLowerCase().includes(q);
      const matchesStatus = status === "all" || (status === "on" ? row.enabled : !row.enabled);
      const matchesDelivery = deliveryStatus === "all" || row.deliveryStatus.toLowerCase() === deliveryStatus;
      const matchesArchived = showArchived || row.deliveryStatus !== "Archived";
      const matchesGroup =
        deliveryGroup === "all" ||
        (deliveryGroup === "healthy" && row.deliveryStatus === "Live") ||
        (deliveryGroup === "warning" && row.deliveryStatus === "Limited") ||
        (deliveryGroup === "inactive" && (row.deliveryStatus === "Paused" || row.deliveryStatus === "Archived"));

      return matchesText && matchesStatus && matchesDelivery && matchesArchived && matchesGroup;
    });
  }, [demandData, filterText, status, deliveryStatus, showArchived, deliveryGroup]);

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Create / Edit modal */}
      {modal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm">
          <Card className="w-full max-w-md rounded-[28px] shadow-2xl">
            <CardHeader>
              <CardTitle>
                {modal.mode === "create" ? "New" : "Edit"}{" "}
                {modal.tab === "supply" ? "Supply Partner" : "Demand Partner"}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4 px-6 pb-6">
              <div>
                <Label>Name</Label>
                <Input
                  value={modal.name}
                  onChange={(e) => setModal((m) => m && { ...m, name: e.target.value })}
                  className="mt-1 rounded-2xl"
                />
              </div>
              <div>
                <Label>Delivery Status</Label>
                <Select
                  value={modal.deliveryStatus}
                  onValueChange={(v) => setModal((m) => m && { ...m, deliveryStatus: v })}
                >
                  <SelectTrigger className="mt-1 rounded-2xl">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Live">Live</SelectItem>
                    <SelectItem value="Limited">Limited</SelectItem>
                    <SelectItem value="Paused">Paused</SelectItem>
                    <SelectItem value="Archived">Archived</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="flex items-center gap-3">
                <Switch
                  checked={modal.active}
                  onCheckedChange={(v) => setModal((m) => m && { ...m, active: v })}
                />
                <Label>Active</Label>
              </div>
              <div className="flex gap-2 pt-2">
                <Button
                  onClick={handleModalSubmit}
                  disabled={saving || !modal.name.trim()}
                  className="flex-1 rounded-2xl"
                >
                  {saving ? "Saving…" : modal.mode === "create" ? "Create" : "Save"}
                </Button>
                <Button
                  variant="outline"
                  onClick={() => setModal(null)}
                  disabled={saving}
                  className="rounded-2xl"
                >
                  Cancel
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      )}
      <div className="grid min-h-screen grid-cols-1 xl:grid-cols-[280px_1fr]">
        <aside className="border-r bg-white p-5">
          <div className="mb-6 flex items-center gap-3 px-2">
            <div className="rounded-2xl bg-primary p-3 text-primary-foreground">
              <LayoutDashboard className="h-5 w-5" />
            </div>
            <div>
              <div className="text-lg font-semibold">Revenue Console</div>
              <div className="text-xs text-muted-foreground">Supply & Demand Manager</div>
            </div>
          </div>

          <div className="space-y-2">
            <button className="flex w-full items-center gap-3 rounded-2xl bg-primary px-3 py-2 text-left text-sm font-medium text-primary-foreground">
              <LayoutDashboard className="h-4 w-4" />
              Dashboard
            </button>

            <SidebarSection
              icon={Building2}
              title="Publisher manager"
              items={["Supply partners", "Publishers", "Channels", "Ad Units"]}
            />

            <SidebarSection
              icon={ShoppingCart}
              title="Demand manager"
              items={["Demand partners", "Demands", "Demand rules"]}
            />
          </div>
        </aside>

        <main className="p-4 md:p-6 xl:p-8">
          <div className="mb-6 flex flex-col gap-2">
            <h1 className="text-3xl font-semibold tracking-tight">Dashboard</h1>
            <p className="text-sm text-muted-foreground">
              Manage supply partners, publishers, channels, ad units, demand partners, demands, and demand rules in one operational view.
            </p>
          </div>

          <FiltersBar
            filterText={filterText}
            setFilterText={setFilterText}
            status={status}
            setStatus={setStatus}
            showArchived={showArchived}
            setShowArchived={setShowArchived}
            deliveryStatus={deliveryStatus}
            setDeliveryStatus={setDeliveryStatus}
            deliveryGroup={deliveryGroup}
            setDeliveryGroup={setDeliveryGroup}
            onCreateNew={() => activeTab !== "reports" && openCreate(activeTab as "supply" | "demand")}
          />

          {loading && (
            <div className="mt-8 text-center text-sm text-muted-foreground">Loading dashboard data…</div>
          )}
          {fetchError && (
            <div className="mt-4 rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">
              {fetchError} —{" "}
              <button className="underline" onClick={fetchData}>
                retry
              </button>
            </div>
          )}

          <Tabs
            defaultValue="supply"
            className="mt-6 space-y-6"
            onValueChange={(v) => setActiveTab(v as "supply" | "demand" | "reports")}
          >
            <TabsList className="grid w-full grid-cols-3 rounded-2xl md:w-[580px]">
              <TabsTrigger value="supply">Supply partners / Publishers</TabsTrigger>
              <TabsTrigger value="demand">Demand partners</TabsTrigger>
              <TabsTrigger value="reports">Bid Reports</TabsTrigger>
            </TabsList>

            <TabsContent value="supply">
              <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-8">
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Opportunities</p>
                    <p className="mt-1 text-lg font-semibold">{formatInteger(supplyTotals.totalOpportunities)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Impressions</p>
                    <p className="mt-1 text-lg font-semibold">{formatInteger(supplyTotals.totalImpressions)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Gross Revenue</p>
                    <p className="mt-1 text-lg font-semibold">{formatCurrency(supplyTotals.totalGrossRevenue)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Publisher Payout</p>
                    <p className="mt-1 text-lg font-semibold">{formatCurrency(supplyTotals.totalPublisherPayout)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Opportunity eCPM</p>
                    <p className="mt-1 text-lg font-semibold">{formatCurrency(supplyTotals.opportunityEcpm)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Fill Rate</p>
                    <p className="mt-1 text-lg font-semibold">{formatPercent(supplyTotals.fillRate)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">VCR</p>
                    <p className="mt-1 text-lg font-semibold">{formatPercent(supplyTotals.vcr)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Viewability</p>
                    <p className="mt-1 text-lg font-semibold">{formatPercent(supplyTotals.viewability)}</p>
                  </CardContent>
                </Card>
              </div>
              <Card className="rounded-[28px] shadow-sm">
                <CardHeader>
                  <CardTitle>Supply partners / Publishers</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-auto rounded-2xl border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Off/On</TableHead>
                          <TableHead>Id</TableHead>
                          <TableHead>Name</TableHead>
                          <TableHead>Delivery Status</TableHead>
                          <TableHead>Opportunities</TableHead>
                          <TableHead>Gross Revenue($)</TableHead>
                          <TableHead>Opportunity ECPM</TableHead>
                          <TableHead>Opportunity Fill Rate %</TableHead>
                          <TableHead>Avg QPS Yesterday</TableHead>
                          <TableHead>Avg QPS Last Hour</TableHead>
                          <TableHead>Impressions</TableHead>
                          <TableHead>VCR %</TableHead>
                          <TableHead>Viewability %</TableHead>
                          <TableHead>Publisher Payout($)</TableHead>
                          <TableHead>Actions</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredSupply.map((row) => (
                          <TableRow key={row.id}>
                            <TableCell><Switch checked={row.enabled} onCheckedChange={(v) => handleToggle("supply", row.id, v)} /></TableCell>
                            <TableCell className="font-medium">{row.id}</TableCell>
                            <TableCell>{row.name}</TableCell>
                            <TableCell><StatusBadge status={row.deliveryStatus} /></TableCell>
                            <TableCell>{formatInteger(row.opportunities)}</TableCell>
                            <TableCell>{formatCurrency(row.grossRevenue)}</TableCell>
                            <TableCell>{formatCurrency(row.opportunityEcpm)}</TableCell>
                            <TableCell>{formatPercent(row.opportunityFillRate)}</TableCell>
                            <TableCell>{formatQps(row.avgQpsYesterday)}</TableCell>
                            <TableCell>{formatQps(row.avgQpsLastHour)}</TableCell>
                            <TableCell>{formatInteger(row.impressions)}</TableCell>
                            <TableCell>{formatPercent(row.impressions > 0 ? (row.completions / row.impressions) * 100 : 0)}</TableCell>
                            <TableCell>{formatPercent(row.impressions > 0 ? (row.viewableImpressions / row.impressions) * 100 : 0)}</TableCell>
                            <TableCell>{formatCurrency(row.publisherPayout)}</TableCell>
                            <TableCell>
                              <Button
                                variant="outline"
                                size="sm"
                                className="rounded-xl"
                                onClick={() => {
                                  const api = supplyApi.find((p) => p.id === row.id);
                                  if (api) openEdit("supply", api);
                                }}
                              >
                                Edit
                              </Button>
                            </TableCell>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="demand">
              <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-9">
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Bid Requests</p>
                    <p className="mt-1 text-lg font-semibold">{formatInteger(demandTotals.totalBidRequests)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Bids</p>
                    <p className="mt-1 text-lg font-semibold">{formatInteger(demandTotals.totalBids)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Impressions</p>
                    <p className="mt-1 text-lg font-semibold">{formatInteger(demandTotals.totalImpressions)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Gross Revenue</p>
                    <p className="mt-1 text-lg font-semibold">{formatCurrency(demandTotals.totalGrossRevenue)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Payout</p>
                    <p className="mt-1 text-lg font-semibold">{formatCurrency(demandTotals.totalPayout)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Bid Req. eCPM</p>
                    <p className="mt-1 text-lg font-semibold">{formatCurrency(demandTotals.bidRequestEcpm)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Win eCPM</p>
                    <p className="mt-1 text-lg font-semibold">{formatCurrency(demandTotals.winEcpm)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">VCR</p>
                    <p className="mt-1 text-lg font-semibold">{formatPercent(demandTotals.vcr)}</p>
                  </CardContent>
                </Card>
                <Card className="rounded-2xl shadow-sm">
                  <CardContent className="p-4">
                    <p className="text-xs text-muted-foreground">Viewability</p>
                    <p className="mt-1 text-lg font-semibold">{formatPercent(demandTotals.viewability)}</p>
                  </CardContent>
                </Card>
              </div>
              <Card className="rounded-[28px] shadow-sm">
                <CardHeader>
                  <CardTitle>Demand partners</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-auto rounded-2xl border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Off/On</TableHead>
                          <TableHead>Id</TableHead>
                          <TableHead>Name</TableHead>
                          <TableHead>Delivery Status</TableHead>
                          <TableHead>Bid Requests</TableHead>
                          <TableHead>Bid Request Fill Rate %</TableHead>
                          <TableHead>Bid Request eCPM</TableHead>
                          <TableHead>Win eCPM</TableHead>
                          <TableHead>Bids</TableHead>
                          <TableHead>Avg QPS Yesterday</TableHead>
                          <TableHead>Avg QPS Last Hour</TableHead>
                          <TableHead>Impressions</TableHead>
                          <TableHead>Gross Revenue($)</TableHead>
                          <TableHead>VCR %</TableHead>
                          <TableHead>Viewability %</TableHead>
                          <TableHead>Payout($)</TableHead>
                          <TableHead>Actions</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredDemand.map((row) => (
                          <TableRow key={row.id}>
                            <TableCell><Switch checked={row.enabled} onCheckedChange={(v) => handleToggle("demand", row.id, v)} /></TableCell>
                            <TableCell className="font-medium">{row.id}</TableCell>
                            <TableCell>{row.name}</TableCell>
                            <TableCell><StatusBadge status={row.deliveryStatus} /></TableCell>
                            <TableCell>{formatInteger(row.bidRequests)}</TableCell>
                            <TableCell>{formatPercent(row.bidRequestFillRate)}</TableCell>
                            <TableCell>{formatCurrency(row.bidRequestEcpm)}</TableCell>
                            <TableCell>{formatCurrency(row.winEcpm)}</TableCell>
                            <TableCell>{formatInteger(row.bids)}</TableCell>
                            <TableCell>{formatQps(row.avgQpsYesterday)}</TableCell>
                            <TableCell>{formatQps(row.avgQpsLastHour)}</TableCell>
                            <TableCell>{formatInteger(row.impressions)}</TableCell>
                            <TableCell>{formatCurrency(row.grossRevenue)}</TableCell>
                            <TableCell>{formatPercent(row.impressions > 0 ? (row.completions / row.impressions) * 100 : 0)}</TableCell>
                            <TableCell>{formatPercent(row.impressions > 0 ? (row.viewableImpressions / row.impressions) * 100 : 0)}</TableCell>
                            <TableCell>{formatCurrency(row.payout)}</TableCell>
                            <TableCell>
                              <Button
                                variant="outline"
                                size="sm"
                                className="rounded-xl"
                                onClick={() => {
                                  const api = demandApi.find((p) => p.id === row.id);
                                  if (api) openEdit("demand", api);
                                }}
                              >
                                Edit
                              </Button>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="reports">
              {/* Report filter bar */}
              <Card className="mb-4 rounded-[28px] shadow-sm">
                <CardContent className="p-4">
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
                    <Input
                      placeholder="Campaign ID"
                      value={reportFilters.campaign_id}
                      onChange={(e) => setReportFilters((f) => ({ ...f, campaign_id: e.target.value }))}
                      className="rounded-2xl"
                    />
                    <Input
                      placeholder="Creative ID (crid)"
                      value={reportFilters.crid}
                      onChange={(e) => setReportFilters((f) => ({ ...f, crid: e.target.value }))}
                      className="rounded-2xl"
                    />
                    <Input
                      placeholder="Ad Domain"
                      value={reportFilters.adomain}
                      onChange={(e) => setReportFilters((f) => ({ ...f, adomain: e.target.value }))}
                      className="rounded-2xl"
                    />
                    <Input
                      placeholder="Bidder"
                      value={reportFilters.bidder}
                      onChange={(e) => setReportFilters((f) => ({ ...f, bidder: e.target.value }))}
                      className="rounded-2xl"
                    />
                    <Select value={reportFilters.event_type || "all"} onValueChange={(v) => setReportFilters((f) => ({ ...f, event_type: v === "all" ? "" : v }))}>
                      <SelectTrigger className="rounded-2xl">
                        <SelectValue placeholder="Event Type" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">Event Type: All</SelectItem>
                        <SelectItem value="bid">Bid</SelectItem>
                        <SelectItem value="win">Win</SelectItem>
                        <SelectItem value="impression">Impression</SelectItem>
                        <SelectItem value="loss">Loss</SelectItem>
                        <SelectItem value="error">Error</SelectItem>
                      </SelectContent>
                    </Select>
                    <Input
                      placeholder="Publisher ID"
                      value={reportFilters.publisher_id}
                      onChange={(e) => setReportFilters((f) => ({ ...f, publisher_id: e.target.value }))}
                      className="rounded-2xl"
                    />
                  </div>
                  <div className="mt-3 flex gap-2">
                    <Button className="rounded-2xl" onClick={() => void fetchReports()} disabled={reportLoading}>
                      {reportLoading ? "Loading…" : "Search"}
                    </Button>
                    <Button variant="outline" className="rounded-2xl" onClick={() => { setReportFilters({ campaign_id: "", crid: "", adomain: "", bidder: "", event_type: "", publisher_id: "" }); }}>
                      Clear
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Report table */}
              <Card className="rounded-[28px] shadow-sm">
                <CardHeader>
                  <CardTitle>Bid Reports ({reports.length})</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-auto rounded-2xl border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Event Type</TableHead>
                          <TableHead>Bidder</TableHead>
                          <TableHead>Ad Domain</TableHead>
                          <TableHead>Creative ID</TableHead>
                          <TableHead>Campaign ID</TableHead>
                          <TableHead>Price</TableHead>
                          <TableHead>Currency</TableHead>
                          <TableHead>Publisher ID</TableHead>
                          <TableHead>Ad Unit ID</TableHead>
                          <TableHead>Domain</TableHead>
                          <TableHead>Country</TableHead>
                          <TableHead>Environment</TableHead>
                          <TableHead>Request ID</TableHead>
                          <TableHead>Time</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {reports.length === 0 && !reportLoading && (
                          <TableRow>
                            <TableCell colSpan={14} className="text-center text-sm text-muted-foreground py-8">
                              No report entries found. Use the filters above and click Search.
                            </TableCell>
                          </TableRow>
                        )}
                        {reports.map((r) => (
                          <TableRow key={r.id}>
                            <TableCell>
                              <Badge variant={r.event_type === "win" || r.event_type === "impression" ? "default" : r.event_type === "error" ? "destructive" : "secondary"} className="rounded-xl capitalize">
                                {r.event_type}
                              </Badge>
                            </TableCell>
                            <TableCell className="font-medium">{r.bidder}</TableCell>
                            <TableCell>{Array.isArray(r.adomain) ? r.adomain.join(", ") : r.adomain}</TableCell>
                            <TableCell>{r.crid}</TableCell>
                            <TableCell>{r.campaign_id}</TableCell>
                            <TableCell>{r.price != null ? formatCurrency(r.price) : "—"}</TableCell>
                            <TableCell>{r.currency || "USD"}</TableCell>
                            <TableCell>{r.publisher_id}</TableCell>
                            <TableCell>{r.ad_unit_id}</TableCell>
                            <TableCell>{r.domain || r.app_bundle}</TableCell>
                            <TableCell>{r.country_code}</TableCell>
                            <TableCell>{r.env}</TableCell>
                            <TableCell className="font-mono text-xs">{r.request_id}</TableCell>
                            <TableCell className="text-xs text-muted-foreground">{r.event_time ? new Date(r.event_time).toLocaleString() : "—"}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </main>
      </div>
    </div>
  );
}
