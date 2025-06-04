import arxiv
import csv

# Seus grupos de termos
group1 = [
    "neural image coding", "neural image compression", "neural image codec",
    "learned image coding", "learned image compression", "learned image codec",
    "deep image coding", "deep image compression", "deep image codec",
    "learning image coding", "learning image compression", "learning image codec",
    "end-to-end image coding", "end-to-end image compression", "end-to-end image codec"
]

group2 = [
    "memory footprint", "memory usage", "memory access", "model size", "memory efficiency",
    "resource utilization", "computational efficiency", "energy efficiency", "energy consumption",
    "power consumption", "power dissipation", "cross-platform", "cross-device",
    "platform-agnostic", "deployment portability", "different hardware", "different platforms",
    "round-off error", "low-latency", "inference efficiency", "real time", "real-time",
    "hardware design", "hardware acceleration", "dedicated hardware", "FPGA", "ASIC",
    "embedded system", "hardware-friendly", "quantization", "integerization", "integer network",
    "bit-width reduction", "precision reduction", "mixed-precision", "pruning",
    "weight sharing", "knowledge distillation", "transfer learning", "model optimization",
    "parameter reduction", "model compression"
]

def create_or_group(terms):
    return " OR ".join([f'(ti:"{t}" OR abs:"{t}")' for t in terms])

group1_query = create_or_group(group1)
group2_query = create_or_group(group2)

final_query = f"({group1_query}) AND ({group2_query})"

print("Query final:\n", final_query)

search = arxiv.Search(
    query=final_query,
    max_results=1000,
    sort_by=arxiv.SortCriterion.Relevance
)

# Abre arquivo CSV para escrever
with open("arxiv_results.csv", mode="w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    # Cabeçalho
    writer.writerow(["Title", "Authors", "Published", "URL", "Abstract"])

    for result in search.results():
        title = result.title.strip()
        authors = ", ".join(a.name for a in result.authors)
        published = result.published.date()
        url = result.entry_id
        abstract = result.summary.replace('\n', ' ').strip()

        writer.writerow([title, authors, published, url, abstract])

print("Exportação para arxiv_results.csv concluída!")
