import { useEffect } from "react";
import PageBreadcrumb from "@/components/common/PageBreadcrumb";

type MigrationPlaceholderPageProps = {
  title: string;
  description: string;
  sourcePaths: string[];
};

export default function MigrationPlaceholderPage({
  title,
  description,
  sourcePaths,
}: MigrationPlaceholderPageProps) {
  useEffect(() => {
    document.title = `OSINT Platform - ${title}`;
  }, [title]);

  return (
    <div>
      <PageBreadcrumb pageTitle={title} />

      <div className="grid grid-cols-12 gap-4 md:gap-6">
        <section className="col-span-12 rounded-2xl border border-gray-200 bg-white p-6 shadow-theme-sm dark:border-gray-800 dark:bg-gray-900">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <p className="text-sm font-semibold uppercase tracking-[0.18em] text-brand-600 dark:text-brand-300">
                Migration slice
              </p>
              <h3 className="mt-2 text-2xl font-semibold text-gray-900 dark:text-white">{title}</h3>
              <p className="mt-3 max-w-2xl text-sm leading-6 text-gray-600 dark:text-gray-400">
                {description}
              </p>
            </div>

            <div className="rounded-2xl border border-brand-100 bg-brand-50 px-4 py-3 text-sm text-brand-700 dark:border-brand-500/20 dark:bg-brand-500/10 dark:text-brand-200">
              Shared shell is live in Vite.
            </div>
          </div>

          <div className="mt-6 grid gap-4 lg:grid-cols-2">
            <article className="rounded-2xl border border-gray-200 bg-gray-50 p-5 dark:border-gray-800 dark:bg-white/[0.03]">
              <h4 className="text-sm font-semibold uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">
                Current source mapping
              </h4>
              <ul className="mt-4 space-y-2 text-sm text-gray-700 dark:text-gray-300">
                {sourcePaths.map((sourcePath) => (
                  <li key={sourcePath} className="rounded-lg border border-gray-200 bg-white px-3 py-2 dark:border-gray-800 dark:bg-gray-900">
                    {sourcePath}
                  </li>
                ))}
              </ul>
            </article>

            <article className="rounded-2xl border border-gray-200 bg-gray-50 p-5 dark:border-gray-800 dark:bg-white/[0.03]">
              <h4 className="text-sm font-semibold uppercase tracking-[0.16em] text-gray-500 dark:text-gray-400">
                Next implementation steps
              </h4>
              <ol className="mt-4 space-y-3 text-sm text-gray-700 dark:text-gray-300">
                <li className="rounded-lg border border-gray-200 bg-white px-3 py-2 dark:border-gray-800 dark:bg-gray-900">
                  Port the domain component into `frontend/src/components`.
                </li>
                <li className="rounded-lg border border-gray-200 bg-white px-3 py-2 dark:border-gray-800 dark:bg-gray-900">
                  Replace App Router params and links with React Router hooks.
                </li>
                <li className="rounded-lg border border-gray-200 bg-white px-3 py-2 dark:border-gray-800 dark:bg-gray-900">
                  Reuse the Vite API client with `VITE_API_URL` and add websocket bindings where needed.
                </li>
              </ol>
            </article>
          </div>
        </section>
      </div>
    </div>
  );
}