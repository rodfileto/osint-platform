"""
Test: Semantic topic analysis of finep.projetos_operacao_direta (full corpus)

Text source: resumo_publicavel (falls back to titulo when NULL or empty)

Pipeline:
  1. Fetch ALL rows (titulo + resumo_publicavel) from Postgres
  2. Build analysis text: resumo_publicavel if non-empty, else titulo
  3. Normalize casing (title case) for better spaCy POS tagging
  4. Embed with sentence-transformers (paraphrase-multilingual-mpnet-base-v2)
  5. Topic modeling via BERTopic (UMAP 5D → HDBSCAN → c-TF-IDF keyword labels)
  6. Build k-NN similarity graph (sparse, above cosine threshold)
  7. Outputs:
       finep_resumo_topics.png   — UMAP 2D scatter colored by topic
       finep_resumo_network.html — interactive pyvis network (stratified sample)

Setup (one-time):
    pip install spacy sentence-transformers bertopic umap-learn hdbscan matplotlib scikit-learn pyvis
    python -m spacy download pt_core_news_lg
"""

import os
import sys
from collections import Counter
from pathlib import Path

import psycopg2

SCRIPT_DIR = Path(__file__).parent

# ── connection ────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "15432")),
    "dbname":   os.getenv("POSTGRES_DB",   "osint_metadata_dev"),
    "user":     os.getenv("POSTGRES_USER", "osint_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "osint_dev_password"),
}

# Graph parameters
SIM_THRESHOLD  = 0.70   # minimum cosine similarity for an edge
K_NEIGHBORS    = 5      # k-NN neighbours per document
NETWORK_SAMPLE = 600    # max nodes in interactive HTML (stratified per topic)


def fetch_rows() -> list[tuple[int, str, str | None]]:
    """Return (id, titulo, resumo_publicavel) for all rows with a non-empty titulo."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, titulo, resumo_publicavel
                FROM finep.projetos_operacao_direta
                WHERE titulo IS NOT NULL AND titulo <> ''
                ORDER BY id
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


# ── spaCy noun phrases (optional, skipped for large corpora) ─────────────────
def load_spacy_model():
    import spacy
    for model_name in ("pt_core_news_trf", "pt_core_news_lg", "pt_core_news_sm"):
        try:
            nlp = spacy.load(model_name)
            print(f"[spaCy] loaded: {model_name}")
            return nlp
        except OSError:
            continue
    print(
        "\n[ERROR] No Portuguese spaCy model found.\n"
        "  python -m spacy download pt_core_news_lg\n"
    )
    sys.exit(1)


def extract_noun_phrases(nlp, titulos: list[str]) -> list[list[str]]:
    results = []
    for doc in nlp.pipe(titulos, batch_size=64):
        chunks = [c.text.strip().lower() for c in doc.noun_chunks if len(c.text.strip()) > 2]
        results.append(chunks)
    return results


# ── BERTopic + network ────────────────────────────────────────────────────────
def run_topic_network(ids: list[int], titulos: list[str], titulos_raw: list[str], texts_raw: list[str]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import numpy as np
    from sentence_transformers import SentenceTransformer
    from bertopic import BERTopic
    from umap import UMAP
    from hdbscan import HDBSCAN
    from sklearn.neighbors import NearestNeighbors

    n = len(titulos)

    # ── 1. embed ──────────────────────────────────────────────────────────────
    print(f"\n── Embedding {n} titles ─────────────────────────────────────────")
    st_model = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2")
    embeddings = st_model.encode(titulos, show_progress_bar=True, batch_size=32)

    # ── 2. BERTopic ───────────────────────────────────────────────────────────
    # Separate UMAP instances: 5D for clustering, 2D for visualization
    print("\n── Running BERTopic ─────────────────────────────────────────────")
    from sklearn.feature_extraction.text import CountVectorizer
    from bertopic.vectorizers import ClassTfidfTransformer
    from bertopic.representation import KeyBERTInspired

    # Portuguese stopwords: NLTK list + common academic filler words
    PT_STOPWORDS = [
        "de", "da", "do", "das", "dos", "em", "no", "na", "nos", "nas",
        "a", "o", "as", "os", "um", "uma", "uns", "umas",
        "para", "por", "pelo", "pela", "pelos", "pelas",
        "com", "sem", "sob", "sobre", "entre", "até", "após",
        "e", "ou", "mas", "que", "se", "como", "mais", "ao", "à",
        "aos", "às", "its", "num", "numa", "nuns", "numas",
        "este", "esta", "estes", "estas", "esse", "essa", "esses", "essas",
        "aquele", "aquela", "aqueles", "aquelas", "ele", "ela", "eles", "elas",
        "eu", "tu", "nós", "vós", "me", "te", "se", "nos", "vos",
        "meu", "minha", "meus", "minhas", "teu", "tua", "teus", "tuas",
        "seu", "sua", "seus", "suas", "nosso", "nossa", "nossos", "nossas",
        "não", "também", "já", "ainda", "quando", "onde", "porque", "pois",
        "foi", "são", "ser", "ter", "tem", "teve", "há", "via",
        # domain-ubiquitous words that appear in every topic
        "projeto", "projetos", "desenvolvimento", "nacional",
    ]

    vectorizer = CountVectorizer(
        stop_words=PT_STOPWORDS,
        ngram_range=(1, 2),
        min_df=3,
    )
    ctfidf = ClassTfidfTransformer(reduce_frequent_words=True)
    representation = KeyBERTInspired()

    umap_topic = UMAP(n_components=5, random_state=42, min_dist=0.0, n_neighbors=15)
    umap_viz   = UMAP(n_components=2, random_state=42, min_dist=0.1, n_neighbors=15)
    hdbscan_model = HDBSCAN(
        min_cluster_size=max(5, n // 100),
        metric="euclidean",
        prediction_data=True,
    )
    topic_model = BERTopic(
        embedding_model=st_model,
        umap_model=umap_topic,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer,
        ctfidf_model=ctfidf,
        representation_model=representation,
        calculate_probabilities=False,
        verbose=True,
    )
    topics, _ = topic_model.fit_transform(titulos, embeddings=embeddings)
    topics_arr = np.array(topics)

    print("\n── 2D UMAP projection for visualization ─────────────────────────")
    # umap stubs type fit_transform as float; cast to ndarray at runtime
    pos2d: np.ndarray = np.array(umap_viz.fit_transform(embeddings))  # type: ignore[arg-type]

    topic_info = topic_model.get_topic_info()
    n_topics = len(topic_info[topic_info["Topic"] >= 0])
    n_outliers = int((topics_arr == -1).sum())
    print(f"\n  {n_topics} topics found | {n_outliers} outliers (topic -1)\n")
    print(f"{'Topic':>6}  {'Count':>6}  Keywords")
    print("─" * 70)
    for _, row in topic_info.iterrows():
        if row["Topic"] == -1:
            continue
        # get_topic() stubs include bool; cast to list at runtime
        topic_words: list[tuple[str, float]] = topic_model.get_topic(row["Topic"]) or []  # type: ignore[assignment]
        kws = ", ".join(w for w, _ in topic_words[:6])
        print(f"  {row['Topic']:4d}  {row['Count']:6d}  {kws}")

    # ── 3. build color palette ────────────────────────────────────────────────
    unique_topics = sorted(set(topics))
    import matplotlib
    palette = matplotlib.colormaps["tab20"]
    topic_to_color = {t: palette(i % 20) for i, t in enumerate(unique_topics)}
    topic_to_color[-1] = (0.65, 0.65, 0.65, 0.25)  # outliers: muted grey

    # ── 4. scatter PNG ────────────────────────────────────────────────────────
    print("\n── Saving UMAP scatter (PNG) ────────────────────────────────────")
    fig, ax = plt.subplots(figsize=(20, 15))
    for t in unique_topics:
        mask = topics_arr == t
        ax.scatter(
            pos2d[mask, 0],
            pos2d[mask, 1],
            s=3,
            c=[topic_to_color[t]],
            alpha=0.65 if t != -1 else 0.2,
            linewidths=0,
            label=f"T{t}" if t != -1 else "outliers",
            rasterized=True,
        )
    # annotate each topic centroid with top-3 keywords
    for t in unique_topics:
        if t == -1:
            continue
        mask = topics_arr == t
        cx = float(pos2d[mask, 0].mean())
        cy = float(pos2d[mask, 1].mean())
        topic_words2: list[tuple[str, float]] = topic_model.get_topic(t) or []  # type: ignore[assignment]
        kws = ", ".join(w for w, _ in topic_words2[:3])
        ax.annotate(
            f"T{t}: {kws}",
            (cx, cy),
            fontsize=6,
            ha="center",
            va="center",
            bbox=dict(boxstyle="round,pad=0.25", fc="white", alpha=0.7, lw=0),
        )
    ax.set_title("FINEP Projetos — Tópicos por Resumo/Título (UMAP + BERTopic)", fontsize=14)
    ax.set_xticks([])
    ax.set_yticks([])
    scatter_path = SCRIPT_DIR / "finep_resumo_topics.png"
    fig.savefig(scatter_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  → {scatter_path}")

    # ── 5. k-NN similarity graph ──────────────────────────────────────────────
    print(f"\n── Building k-NN graph (k={K_NEIGHBORS}, sim≥{SIM_THRESHOLD}) ──────────────────")
    nn_model = NearestNeighbors(n_neighbors=K_NEIGHBORS + 1, metric="cosine", algorithm="brute")
    nn_model.fit(embeddings)
    distances, indices = nn_model.kneighbors(embeddings)

    edges: list[tuple[int, int, float]] = []
    seen: set[tuple[int, int]] = set()
    for i, (nbrs, dists) in enumerate(zip(indices, distances)):
        for j, d in zip(nbrs[1:], dists[1:]):
            sim = float(1.0 - d)
            if sim >= SIM_THRESHOLD:
                key = (min(i, j), max(i, j))
                if key not in seen:
                    seen.add(key)
                    edges.append((i, j, sim))
    print(f"  {len(edges)} edges above threshold")

    # ── 6. interactive pyvis HTML ─────────────────────────────────────────────
    print(f"\n── Saving interactive network (pyvis, ≤{NETWORK_SAMPLE} nodes) ────────────")
    try:
        from pyvis.network import Network
    except ImportError:
        print("  [skip] pyvis not installed: pip install pyvis")
        return

    # stratified sample: proportional per topic
    rng = np.random.default_rng(42)
    sampled_idx: list[int] = []
    for t, count in Counter(topics).items():
        t_indices = np.where(topics_arr == t)[0]
        k = max(1, int(NETWORK_SAMPLE * count / n))
        chosen = rng.choice(t_indices, size=min(k, len(t_indices)), replace=False)
        sampled_idx.extend(chosen.tolist())
    sampled_idx = sorted(sampled_idx[:NETWORK_SAMPLE])
    sampled_set = set(sampled_idx)
    print(f"  {len(sampled_idx)} nodes sampled")

    # scale UMAP 2D positions → pyvis canvas [100, 1900]
    xs = pos2d[sampled_idx, 0]
    ys = pos2d[sampled_idx, 1]
    x_min, x_max = float(xs.min()), float(xs.max())
    y_min, y_max = float(ys.min()), float(ys.max())

    def scale(v: float, vmin: float, vmax: float, lo: float = 100, hi: float = 1900) -> float:
        return lo + (v - vmin) / (vmax - vmin + 1e-9) * (hi - lo)

    def rgba_to_hex(rgba: tuple) -> str:
        r, g, b, _ = rgba
        return "#{:02x}{:02x}{:02x}".format(int(r * 255), int(g * 255), int(b * 255))

    net = Network(height="950px", width="100%", bgcolor="#1a1a1a", font_color="white")  # type: ignore[arg-type]
    net.toggle_physics(False)  # use fixed UMAP coordinates

    for idx in sampled_idx:
        t = topics[idx]
        color = rgba_to_hex(topic_to_color[t])
        resumo_snippet = (texts_raw[idx][:200] + "…") if len(texts_raw[idx]) > 200 else texts_raw[idx]
        tooltip = f"[T{t}] {titulos_raw[idx]}\n{resumo_snippet}"
        net.add_node(
            int(idx),
            label="",
            title=tooltip,
            color=color,
            size=6,
            x=float(scale(float(pos2d[idx, 0]), x_min, x_max)),
            y=float(scale(float(pos2d[idx, 1]), y_min, y_max)),
        )

    sampled_edges = [(i, j, s) for i, j, s in edges if i in sampled_set and j in sampled_set]
    for i, j, sim in sampled_edges:
        net.add_edge(int(i), int(j), value=float(sim), title=f"sim={sim:.3f}", color="#444444", width=0.5)
    print(f"  {len(sampled_edges)} edges in sampled network")

    html_path = SCRIPT_DIR / "finep_resumo_network.html"
    net.save_graph(str(html_path))
    print(f"  → {html_path}")


# ── main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print("── Fetching all rows from finep.projetos_operacao_direta ────────")
    rows = fetch_rows()
    if not rows:
        print("[ERROR] No rows returned. Is the DB loaded?")
        sys.exit(1)

    ids_raw, titulos_raw_tup, resumos_raw_tup = zip(*rows)
    ids = list(ids_raw)
    titulos_raw = list(titulos_raw_tup)
    resumos_raw = list(resumos_raw_tup)

    # Build analysis texts: resumo_publicavel when available, else titulo
    texts_raw = [
        (r.strip() if r and r.strip() else t)
        for r, t in zip(resumos_raw, titulos_raw)
    ]
    n_resumo = sum(1 for r in resumos_raw if r and r.strip())
    n_fallback = len(texts_raw) - n_resumo
    print(f"  fetched {len(texts_raw)} rows  "
          f"({n_resumo} with resumo_publicavel, {n_fallback} fallback to titulo)")

    titulos = [t.title() for t in texts_raw]

    # Noun phrase extraction — only for smaller corpora (slow at full scale)
    if len(titulos) <= 5_000:
        print("\n── Noun phrase extraction (spaCy) ───────────────────────────────")
        nlp = load_spacy_model()
        np_lists = extract_noun_phrases(nlp, titulos)
        all_nps = [np for nps in np_lists for np in nps]
        print("\nMost frequent noun phrases:")
        for phrase, count in Counter(all_nps).most_common(20):
            print(f"  {count:3d}x  {phrase}")
    else:
        print(f"\n[INFO] Skipping spaCy NP extraction ({len(titulos)} rows). Run on a sample separately.")

    run_topic_network(ids, titulos, titulos_raw, texts_raw)


if __name__ == "__main__":
    main()
