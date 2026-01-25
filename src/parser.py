"""Parse rechtspraak.nl XML responses."""

from lxml import etree
from datetime import datetime
from typing import Optional
import re

# Namespaces used in rechtspraak XML
NAMESPACES = {
    "dcterms": "http://purl.org/dc/terms/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "psi": "http://psi.rechtspraak.nl/",
    "rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0",
}


def parse_sitemap(xml_content: bytes) -> list[dict]:
    """Parse sitemap XML and extract ECLI identifiers with lastmod dates."""
    root = etree.fromstring(xml_content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    entries = []
    for url in root.findall(".//sm:url", ns):
        loc = url.findtext("sm:loc", namespaces=ns)
        lastmod = url.findtext("sm:lastmod", namespaces=ns)

        if loc:
            # Extract ECLI from URL like https://uitspraken.rechtspraak.nl/details?id=ECLI:NL:HR:2025:1
            ecli_match = re.search(r"id=(ECLI:[^&]+)", loc)
            if ecli_match:
                entries.append({
                    "ecli": ecli_match.group(1),
                    "lastmod": lastmod,
                    "url": loc,
                })

    return entries


def parse_uitspraak(xml_content: bytes) -> Optional[dict]:
    """Parse uitspraak XML content and extract structured data."""
    try:
        root = etree.fromstring(xml_content)
    except etree.XMLSyntaxError:
        return None

    def get_text(xpath: str) -> Optional[str]:
        elem = root.find(xpath, NAMESPACES)
        return elem.text if elem is not None and elem.text else None

    def get_all_text(xpath: str) -> list[str]:
        elems = root.findall(xpath, NAMESPACES)
        return [e.text for e in elems if e.text]

    # Extract RDF metadata
    rdf = root.find(".//rdf:Description", NAMESPACES)
    if rdf is None:
        rdf = root  # fallback

    ecli = get_text(".//dcterms:identifier") or get_text(".//rs:ecli")

    # Parse dates
    decision_date_str = get_text(".//dcterms:date") or get_text(".//rs:datum")
    publication_date_str = get_text(".//dcterms:issued")

    decision_date = None
    if decision_date_str:
        try:
            decision_date = datetime.fromisoformat(decision_date_str.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                decision_date = datetime.strptime(decision_date_str[:10], "%Y-%m-%d").date()
            except ValueError:
                pass

    publication_date = None
    if publication_date_str:
        try:
            publication_date = datetime.fromisoformat(publication_date_str.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                publication_date = datetime.strptime(publication_date_str[:10], "%Y-%m-%d").date()
            except ValueError:
                pass

    # Court info
    creator = get_text(".//dcterms:creator")
    court = creator or "Unknown"
    court_type = extract_court_type(court)

    # Classification
    procedure = get_text(".//dcterms:type") or get_text(".//psi:procedure")
    subject = get_text(".//dcterms:subject")

    # Case number
    case_number = None
    case_nums = get_all_text(".//psi:zaaknummer")
    if case_nums:
        case_number = case_nums[0]

    # Summary (inhoudsindicatie)
    summary_elem = root.find(".//rs:inhoudsindicatie", NAMESPACES)
    summary = None
    if summary_elem is not None:
        summary = etree.tostring(summary_elem, method="text", encoding="unicode").strip()

    # Related ECLIs
    related = []
    for rel in root.findall(".//dcterms:relation", NAMESPACES):
        ref = rel.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
        if ref and ref.startswith("ECLI:"):
            related.append(ref)

    return {
        "ecli": ecli,
        "case_number": case_number,
        "decision_date": decision_date,
        "publication_date": publication_date,
        "court": court,
        "court_type": court_type,
        "procedure_type": procedure,
        "subject_area": subject,
        "summary": summary,
        "related_eclis": related,
    }


def extract_court_type(court_name: str) -> str:
    """Extract court type from court name."""
    court_lower = court_name.lower()

    if "hoge raad" in court_lower:
        return "HR"
    elif "gerechtshof" in court_lower:
        return "HOF"
    elif "rechtbank" in court_lower:
        return "RB"
    elif "raad van state" in court_lower:
        return "RVS"
    elif "centrale raad van beroep" in court_lower:
        return "CRVB"
    elif "college van beroep" in court_lower:
        return "CBB"
    elif "raad voor de rechtspraak" in court_lower:
        return "RVR"
    else:
        return "OTHER"
