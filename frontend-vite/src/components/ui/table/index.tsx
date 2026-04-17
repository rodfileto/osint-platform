type TableProps = {
  children: React.ReactNode;
  className?: string;
};

type TableSectionProps = {
  children: React.ReactNode;
  className?: string;
};

type TableCellProps = {
  children: React.ReactNode;
  isHeader?: boolean;
  className?: string;
};

export function Table({ children, className }: TableProps) {
  return <table className={`min-w-full ${className ?? ""}`}>{children}</table>;
}

export function TableHeader({ children, className }: TableSectionProps) {
  return <thead className={className}>{children}</thead>;
}

export function TableBody({ children, className }: TableSectionProps) {
  return <tbody className={className}>{children}</tbody>;
}

export function TableRow({ children, className }: TableSectionProps) {
  return <tr className={className}>{children}</tr>;
}

export function TableCell({ children, isHeader = false, className }: TableCellProps) {
  const CellTag = isHeader ? "th" : "td";

  return <CellTag className={className}>{children}</CellTag>;
}