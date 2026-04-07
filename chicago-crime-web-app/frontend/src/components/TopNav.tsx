import { Link, useLocation } from "react-router-dom";
import { Map, MessageSquare, Shield } from "lucide-react";
import { cn } from "@/lib/utils";

type NavKey = "dashboard" | "chat" | "map";

type TopNavProps = {
  /** Tighter header for full-viewport chat/map on small screens */
  variant?: "default" | "dense";
};

export default function TopNav({ variant = "default" }: TopNavProps) {
  const location = useLocation();
  const dense = variant === "dense";

  const active: NavKey | "about" | "architecture" =
    location.pathname === "/map" ? "map" : location.pathname === "/chat" ? "chat" : location.pathname === "/about" ? "about" : location.pathname === "/architecture" ? "architecture" : "dashboard";

  const base = cn(
    "flex items-center justify-center gap-2 rounded-xl transition-all border border-border/70 hover:border-primary/50 hover:bg-secondary/50 shrink-0",
    dense ? "px-2.5 py-2 text-xs min-h-[40px] min-w-[40px] sm:min-w-0 sm:px-3 sm:gap-2" : "px-4 py-2 text-sm"
  );
  const activeClass = "bg-primary/15 text-foreground border-primary/60 shadow-[0_0_0_1px_hsl(var(--primary)/0.15)]";
  const inactiveClass = "bg-transparent text-muted-foreground";

  const iconWrap = "inline-flex items-center justify-center w-5 h-5 rounded-md bg-primary/15 text-primary shrink-0";

  return (
    <header
      className={cn(
        "shrink-0 z-40 border-b border-border/40 bg-background/85 backdrop-blur-md pt-safe",
        "shadow-[0_8px_24px_hsl(220_30%_2%/0.2)]",
        dense ? "px-2 sm:px-4 py-2" : "px-4 sm:px-6 py-4"
      )}
    >
      <div
        className={cn(
          "mx-auto flex min-w-0 items-center gap-2 sm:gap-4",
          dense ? "max-w-none justify-between" : "max-w-6xl flex-col sm:flex-row sm:items-center sm:justify-between gap-4"
        )}
      >
        <div className="flex min-w-0 items-center gap-2">
          <div
            className={cn(
              "inline-flex min-w-0 items-center gap-2 rounded-xl border border-primary/20 bg-primary/10",
              dense ? "px-2 py-1.5 sm:px-3" : "px-3 py-1.5"
            )}
          >
            <img
              src="/logo.svg"
              width={20}
              height={20}
              className="h-5 w-5 shrink-0"
              alt=""
            />
            <span className={cn("truncate font-semibold", dense ? "max-w-[9rem] text-xs sm:max-w-none sm:text-sm" : "text-sm")}>
              Chicago Civic Risk Atlas
            </span>
          </div>
        </div>

        <nav className={cn("flex min-w-0 items-center gap-1.5 sm:gap-2", dense ? "no-scrollbar max-w-[55vw] flex-nowrap overflow-x-auto sm:max-w-none" : "flex-wrap")}>
          <Link
            to="/"
            className={cn(base, active === "dashboard" ? activeClass : inactiveClass)}
            aria-current={active === "dashboard" ? "page" : undefined}
            aria-label="Dashboard"
          >
            <span className={iconWrap}>
              <Shield className="h-3.5 w-3.5" />
            </span>
            <span className={dense ? "hidden sm:inline" : undefined}>Dashboard</span>
          </Link>

          <Link
            to="/chat"
            className={cn(base, active === "chat" ? activeClass : inactiveClass)}
            aria-current={active === "chat" ? "page" : undefined}
            aria-label="Chat"
          >
            <span className={iconWrap}>
              <MessageSquare className="h-3.5 w-3.5" />
            </span>
            <span className={dense ? "hidden sm:inline" : undefined}>Chat</span>
          </Link>

          <Link
            to="/map"
            className={cn(base, active === "map" ? activeClass : inactiveClass)}
            aria-current={active === "map" ? "page" : undefined}
            aria-label="Live Map"
          >
            <span className={iconWrap}>
              <Map className="h-3.5 w-3.5" />
            </span>
            <span className={dense ? "hidden sm:inline" : undefined}>Map</span>
          </Link>

          <Link
            to="/about"
            className={cn(base, active === "about" ? activeClass : inactiveClass)}
            aria-current={active === "about" ? "page" : undefined}
            aria-label="About"
          >
            <span className={iconWrap}>
              <MessageSquare className="h-3.5 w-3.5" /> 
            </span>
            <span className={dense ? "hidden sm:inline" : undefined}>About</span>
          </Link>

          <Link
            to="/architecture"
            className={cn(base, active === "architecture" ? activeClass : inactiveClass)}
            aria-current={active === "architecture" ? "page" : undefined}
            aria-label="Architecture"
          >
            <span className={iconWrap}>
              <Map className="h-3.5 w-3.5" /> 
            </span>
            <span className={dense ? "hidden sm:inline" : undefined}>Architecture</span>
          </Link>
        </nav>
      </div>
    </header>
  );
}

