type BadgeVariant = "light" | "solid";
type BadgeSize = "sm" | "md";
type BadgeColor = "primary" | "success" | "error" | "warning" | "info" | "light" | "dark";

type BadgeProps = {
  variant?: BadgeVariant;
  size?: BadgeSize;
  color?: BadgeColor;
  startIcon?: React.ReactNode;
  endIcon?: React.ReactNode;
  children: React.ReactNode;
};

export default function Badge({
  variant = "light",
  size = "md",
  color = "primary",
  startIcon,
  endIcon,
  children,
}: BadgeProps) {
  const baseStyles = "inline-flex items-center justify-center gap-1 rounded-full px-2.5 py-0.5 font-medium";

  const sizeStyles = {
    sm: "text-theme-xs",
    md: "text-sm",
  };

  const variants = {
    light: {
      primary: "bg-brand-50 text-brand-500 dark:bg-brand-500/15 dark:text-brand-400",
      success: "bg-emerald-50 text-emerald-600 dark:bg-emerald-500/15 dark:text-emerald-400",
      error: "bg-red-50 text-red-600 dark:bg-red-500/15 dark:text-red-400",
      warning: "bg-amber-50 text-amber-600 dark:bg-amber-500/15 dark:text-amber-400",
      info: "bg-sky-50 text-sky-600 dark:bg-sky-500/15 dark:text-sky-400",
      light: "bg-gray-100 text-gray-700 dark:bg-white/5 dark:text-white/80",
      dark: "bg-gray-500 text-white dark:bg-white/5 dark:text-white",
    },
    solid: {
      primary: "bg-brand-500 text-white",
      success: "bg-emerald-500 text-white",
      error: "bg-red-500 text-white",
      warning: "bg-amber-500 text-white",
      info: "bg-sky-500 text-white",
      light: "bg-gray-400 text-white",
      dark: "bg-gray-700 text-white",
    },
  };

  return (
    <span className={`${baseStyles} ${sizeStyles[size]} ${variants[variant][color]}`}>
      {startIcon ? <span className="mr-1">{startIcon}</span> : null}
      {children}
      {endIcon ? <span className="ml-1">{endIcon}</span> : null}
    </span>
  );
}