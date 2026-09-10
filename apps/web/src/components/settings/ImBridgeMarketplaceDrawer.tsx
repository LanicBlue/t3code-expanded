/**
 * Bundled template marketplace for the IM Bridge member roster — the hive
 * recipe ported in miniature: a curated, read-only snapshot of agent persona
 * prompts (jnMetaCode/agency-agents-zh, MIT) browsable by category, whose
 *「导入」prefills a new (disabled) member row. The snapshot ships as a public
 * asset fetched lazily on first open, so the 2.6 MB payload never touches the
 * main bundle. No server API, no persistence — the member table remains the
 * single source of truth.
 *
 * @module ImBridgeMarketplaceDrawer
 */
import { useEffect, useMemo, useState } from "react";

import { Button } from "../ui/button";
import {
  Dialog,
  DialogDescription,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { Input } from "../ui/input";
import { cn } from "../../lib/utils";

/** Shape of public/imbridge-marketplace.zh.json (see scripts/gen-imbridge-marketplace.mjs). */
interface MarketplaceAgent {
  readonly path: string;
  readonly category: string;
  readonly name: string;
  readonly description: string;
  readonly emoji: string | null;
  readonly color: string | null;
  readonly vibe: string | null;
  readonly prompt: string;
}
interface MarketplaceData {
  readonly source: { readonly repo: string; readonly commit: string; readonly license: string };
  readonly categories: ReadonlyArray<string>;
  readonly agents: ReadonlyArray<MarketplaceAgent>;
}

// Module-level cache: fetch once per page load, share across drawer opens.
let dataPromise: Promise<MarketplaceData> | null = null;
const loadData = () => {
  dataPromise ??= fetch(`${import.meta.env.BASE_URL}imbridge-marketplace.zh.json`).then((res) => {
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json() as Promise<MarketplaceData>;
  });
  return dataPromise;
};

const CATEGORY_LABELS: Record<string, string> = {
  academic: "学术",
  design: "设计",
  engineering: "工程",
  finance: "金融",
  "game-development": "游戏开发",
  hr: "人力资源",
  legal: "法律",
  marketing: "市场营销",
  "paid-media": "付费媒体",
  product: "产品",
  "project-management": "项目管理",
  sales: "销售",
  "spatial-computing": "空间计算",
  specialized: "专业领域",
  "supply-chain": "供应链",
  support: "客户支持",
  testing: "测试",
};

export function ImBridgeMarketplaceDrawer({
  open,
  onOpenChange,
  onImport,
}: {
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  /** Receives the chosen template; the parent appends the member row. */
  readonly onImport: (template: { name: string; prompt: string }) => void;
}) {
  const [data, setData] = useState<MarketplaceData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [category, setCategory] = useState<string>("all");
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<MarketplaceAgent | null>(null);

  useEffect(() => {
    if (!open || data || error) return;
    loadData().then(setData, (err) => setError(String(err?.message ?? err)));
  }, [open, data, error]);

  const agents = useMemo(() => {
    if (!data) return [];
    const lower = query.trim().toLowerCase();
    return data.agents.filter(
      (agent) =>
        (category === "all" || agent.category === category) &&
        (lower.length === 0 ||
          agent.name.toLowerCase().includes(lower) ||
          agent.description.toLowerCase().includes(lower)),
    );
  }, [data, category, query]);

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (next) return;
        setSelected(null);
        onOpenChange(false);
      }}
    >
      <DialogPopup className="max-w-5xl">
        <DialogHeader>
          <DialogTitle>模板市场</DialogTitle>
          <DialogDescription>
            内置的 agent 人设模板快照（{data ? `${data.agents.length} 个 · ` : ""}
            {data?.source.repo ?? "jnMetaCode/agency-agents-zh"} ·
            MIT）。「导入」会新增一个未启用的成员行：人设进成员配置、名称取模板名，id
            自动生成；选好实例 / 模型后再勾选启用。
          </DialogDescription>
        </DialogHeader>
        <DialogPanel>
          {error ? (
            <p className="py-8 text-center text-[13px] text-destructive">
              模板数据加载失败：{error}
            </p>
          ) : !data ? (
            <p className="py-8 text-center text-[13px] text-muted-foreground/80">加载中……</p>
          ) : (
            <div className="flex h-[60vh] gap-3">
              <div className="flex w-44 shrink-0 flex-col gap-2 overflow-y-auto pr-1">
                <Input
                  aria-label="搜索模板"
                  className="mb-1"
                  placeholder="搜索名称 / 描述"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                />
                <CategoryButton
                  active={category === "all"}
                  label={`全部（${data.agents.length}）`}
                  onClick={() => setCategory("all")}
                />
                {data.categories.map((name) => (
                  <CategoryButton
                    key={name}
                    active={category === name}
                    label={`${CATEGORY_LABELS[name] ?? name}（${data.agents.filter((a) => a.category === name).length}）`}
                    onClick={() => setCategory(name)}
                  />
                ))}
              </div>
              <div className="min-w-0 flex-1 overflow-y-auto pr-1">
                <div className="grid grid-cols-1 gap-1.5 sm:grid-cols-2">
                  {agents.map((agent) => (
                    <button
                      key={agent.path}
                      type="button"
                      className={cn(
                        "rounded-md border px-2.5 py-2 text-left transition-colors",
                        selected?.path === agent.path
                          ? "border-primary/70 bg-primary/10"
                          : "border-border/60 hover:bg-accent/50",
                      )}
                      onClick={() => setSelected(agent)}
                    >
                      <p className="truncate text-[13px] font-medium">
                        {agent.emoji ? `${agent.emoji} ` : ""}
                        {agent.name}
                      </p>
                      <p className="mt-0.5 line-clamp-2 text-xs leading-relaxed text-muted-foreground/85">
                        {agent.description}
                      </p>
                    </button>
                  ))}
                  {agents.length === 0 ? (
                    <p className="col-span-full py-6 text-center text-[13px] text-muted-foreground/80">
                      没有匹配的模板。
                    </p>
                  ) : null}
                </div>
              </div>
              <div className="flex w-[38%] shrink-0 flex-col border-l border-border/60 pl-3">
                {selected ? (
                  <>
                    <p className="pb-1 text-[13px] font-medium">
                      {selected.emoji ? `${selected.emoji} ` : ""}
                      {selected.name}
                      <span className="pl-2 text-xs font-normal text-muted-foreground/80">
                        {CATEGORY_LABELS[selected.category] ?? selected.category}
                      </span>
                    </p>
                    {selected.vibe ? (
                      <p className="pb-1 text-xs text-muted-foreground/80">{selected.vibe}</p>
                    ) : null}
                    <pre className="min-h-0 flex-1 overflow-y-auto whitespace-pre-wrap rounded-md bg-muted/50 p-2.5 font-mono text-[11.5px] leading-relaxed">
                      {selected.prompt}
                    </pre>
                    <Button
                      size="xs"
                      className="mt-2 self-end"
                      onClick={() => {
                        onImport({ name: selected.name, prompt: selected.prompt });
                        setSelected(null);
                        onOpenChange(false);
                      }}
                    >
                      导入为成员
                    </Button>
                  </>
                ) : (
                  <p className="py-6 text-center text-[13px] text-muted-foreground/80">
                    选择一个模板查看完整人设。
                  </p>
                )}
              </div>
            </div>
          )}
        </DialogPanel>
      </DialogPopup>
    </Dialog>
  );
}

function CategoryButton({
  active,
  label,
  onClick,
}: {
  readonly active: boolean;
  readonly label: string;
  readonly onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={cn(
        "truncate rounded-md border px-2 py-1.5 text-left text-xs transition-colors",
        active
          ? "border-primary/70 bg-primary/10"
          : "border-border/50 hover:bg-accent/50 dark:bg-white/[0.02]",
      )}
      onClick={onClick}
    >
      {label}
    </button>
  );
}
