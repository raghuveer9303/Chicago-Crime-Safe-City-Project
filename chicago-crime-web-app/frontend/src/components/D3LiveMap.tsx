import { useEffect, useMemo, useRef, useState } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

interface AreaPrediction {
  area_id: number;
  name: string | null;
  safety_score: number;
  predicted_count: number;
  predicted_count_low: number | null;
  predicted_count_high: number | null;
  top_categories?: Array<{ label: string; rank: number }> | null;
}

interface GeoFeature {
  type: "Feature";
  properties: {
    area_id?: number | string;
    name?: string | null;
    [key: string]: unknown;
  };
  geometry: {
    type: string;
    coordinates: unknown;
  };
}

interface AreaGeoJSON {
  type: "FeatureCollection";
  features: GeoFeature[];
}

const CHICAGO_BOUNDS = L.latLngBounds([41.64, -87.94], [42.02, -87.52]);

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function getAreaNum(feature: GeoFeature): number {
  const raw =
    feature?.properties?.area_id ??
    (feature?.properties as Record<string, unknown>)?.area_numbe ??
    (feature?.properties as Record<string, unknown>)?.area_num_1 ??
    0;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  return Number.isFinite(n) ? n : 0;
}

function getSafetyColor(score: number): string {
  if (score >= 70) return "hsl(142, 70%, 45%)";
  if (score >= 40) return "hsl(45, 90%, 55%)";
  return "hsl(0, 72%, 55%)";
}

function getFallbackAreaColor(areaNum: number): string {
  const hue = ((areaNum || 1) * 31) % 360;
  return `hsl(${hue}, 78%, 55%)`;
}

export default function D3LiveMap() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const geoLayerRef = useRef<L.GeoJSON | null>(null);

  const [geojson, setGeojson] = useState<AreaGeoJSON | null>(null);
  const [predictions, setPredictions] = useState<AreaPrediction[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [processDate, setProcessDate] = useState<string>("");
  const [predictionDate, setPredictionDate] = useState<string>("");

  const predMap = useMemo(() => {
    const m = new Map<number, AreaPrediction>();
    predictions.forEach((p) => m.set(p.area_id, p));
    return m;
  }, [predictions]);

  const fmt = useMemo(() => new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }), []);

  useEffect(() => {
    let cancelled = false;

    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const [geoRes, predRes] = await Promise.all([fetch("/api/ml/community-areas"), fetch("/api/ml/areas")]);
        if (!geoRes.ok || !predRes.ok) throw new Error("Failed to fetch data");
        const geoData = (await geoRes.json()) as AreaGeoJSON;
        const predData = await predRes.json();
        const areas: AreaPrediction[] = Array.isArray(predData) ? predData : predData.areas || [];
        if (cancelled) return;
        setGeojson(geoData);
        setPredictions(areas);
        setProcessDate(predData?.meta?.process_date || "");
        setPredictionDate(predData?.meta?.prediction_date || "");
      } catch (e: unknown) {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : "Unknown error");
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void fetchData();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    const map = L.map(containerRef.current, {
      zoomControl: true,
      scrollWheelZoom: true,
      minZoom: 10,
      maxZoom: 15,
      maxBounds: CHICAGO_BOUNDS.pad(0.08),
      maxBoundsViscosity: 0.8,
    });

    mapRef.current = map;
    map.fitBounds(CHICAGO_BOUNDS);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    }).addTo(map);

    return () => {
      geoLayerRef.current?.remove();
      mapRef.current?.remove();
      geoLayerRef.current = null;
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !geojson) return;

    geoLayerRef.current?.remove();

    const layer = L.geoJSON(geojson as GeoJSON.GeoJsonObject, {
      style: (feature) => {
        const typedFeature = feature as unknown as GeoFeature;
        const areaNum = getAreaNum(typedFeature);
        const pred = predMap.get(areaNum);
        return {
          fillColor: pred ? getSafetyColor(pred.safety_score) : getFallbackAreaColor(areaNum),
          fillOpacity: 0.72,
          color: "hsl(210, 20%, 85%)",
          opacity: 0.55,
          weight: 1.1,
        };
      },
      onEachFeature: (feature, featureLayer) => {
        const typedFeature = feature as unknown as GeoFeature;
        const areaNum = getAreaNum(typedFeature);
        const pred = predMap.get(areaNum);
        const name = typedFeature?.properties?.name || `Area ${areaNum}`;
        const safetyScore = pred ? String(Math.round(pred.safety_score)) : "N/A";
        const predicted = pred ? fmt.format(pred.predicted_count) : "N/A";
        const low = pred?.predicted_count_low == null ? null : fmt.format(pred.predicted_count_low);
        const high = pred?.predicted_count_high == null ? null : fmt.format(pred.predicted_count_high);
        const bounds = low && high ? `${low}–${high}` : null;
        const top3 =
          pred?.top_categories && Array.isArray(pred.top_categories)
            ? pred.top_categories
                .slice()
                .sort((a, b) => (a.rank ?? 999) - (b.rank ?? 999))
                .slice(0, 3)
                .map((c) => c?.label)
                .filter(Boolean)
            : [];

        featureLayer.bindTooltip(
          `<div class="leaflet-map-tooltip">
            <div class="leaflet-map-tooltip-title">${escapeHtml(String(name))}</div>
            <div class="leaflet-map-tooltip-row">
              <span class="leaflet-map-tooltip-k">Safety</span>
              <span class="leaflet-map-tooltip-v">${escapeHtml(safetyScore)}</span>
            </div>
            <div class="leaflet-map-tooltip-row">
              <span class="leaflet-map-tooltip-k">Predicted</span>
              <span class="leaflet-map-tooltip-v">${escapeHtml(predicted)}${bounds ? ` <span class="leaflet-map-tooltip-muted">(${escapeHtml(bounds)})</span>` : ""}</span>
            </div>
            ${
              top3.length
                ? `<div class="leaflet-map-tooltip-subtitle">Top crimes</div>
                   <ol class="leaflet-map-tooltip-list">
                     ${top3.map((t) => `<li>${escapeHtml(String(t))}</li>`).join("")}
                   </ol>`
                : ""
            }
          </div>`,
          {
            sticky: true,
            direction: "top",
          }
        );

        featureLayer.on("mouseover", () => {
          featureLayer.setStyle({
            fillOpacity: 0.9,
            weight: 2,
          });
        });
        featureLayer.on("mouseout", () => {
          featureLayer.setStyle({
            fillOpacity: 0.72,
            weight: 1.1,
          });
        });
      },
    });

    layer.addTo(map);
    geoLayerRef.current = layer;
  }, [geojson, predMap, fmt]);

  return (
    <div className="flex flex-col gap-5 flex-1 min-h-0 animate-fade-in">
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <h2 className="text-xl font-semibold tracking-tight text-foreground">Live Community Safety Map</h2>
          <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium bg-live/20 text-live border border-live/30">
            <span className="w-1.5 h-1.5 rounded-full bg-live live-pulse" />
            LIVE
          </span>
        </div>
        {processDate && (
          <div className="text-xs text-muted-foreground space-x-3">
            <span>Processed: {processDate}</span>
            <span>Prediction: {predictionDate}</span>
          </div>
        )}
      </div>

      <div className="space-y-2">
        <p className="text-xs text-muted-foreground mt-1">
          Higher Safety Score (0–100) indicates lower expected risk. Hover an area for the exact score.
        </p>
        <p className="text-xs text-muted-foreground">
          <strong>Predicted</strong> is the model-estimated number of crime incidents for that area on the shown
          prediction date (not already reported incidents). If shown, values in parentheses are the expected low-high
          range.
        </p>
      </div>

      <div className="flex-1 relative rounded-xl overflow-hidden glass-panel min-h-[420px]">
        <div ref={containerRef} className="leaflet-map-root absolute inset-0" />
        {loading && (
          <div className="absolute inset-0 z-[1000] flex items-center justify-center bg-background/60 backdrop-blur-sm">
            <div className="flex items-center gap-3 text-muted-foreground">
              <div className="w-5 h-5 border-2 border-primary border-t-transparent rounded-full animate-spin" />
              Loading map data…
            </div>
          </div>
        )}
        {error && (
          <div className="absolute inset-0 z-[1000] flex items-center justify-center bg-background/60 backdrop-blur-sm">
            <p className="text-destructive text-sm">{error}</p>
          </div>
        )}

      </div>

      <div className="flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-sm bg-safety-high" /> High Safety / Low Risk
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-sm bg-safety-mid" /> Moderate
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-sm bg-safety-low" /> Low Safety / High Risk
        </span>
      </div>
    </div>
  );
}

