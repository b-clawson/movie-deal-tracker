"""
Edition classifier using rule-based matching.
Identifies special editions, formats (4K, Blu-ray, DVD), and boutique labels.
No external API required - fast and free.
"""

import re
import logging
from typing import Tuple, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Known boutique labels (case-insensitive matching)
BOUTIQUE_LABELS = [
    # Major boutique labels
    "Criterion Collection", "Criterion",
    "Arrow Video", "Arrow Academy", "Arrow Films",
    "Kino Lorber", "Kino Classics", "Kino Cult",
    "Shout Factory", "Shout! Factory", "Scream Factory", "Shout Select",
    "Vinegar Syndrome", "VS",

    # UK boutique labels
    "BFI", "British Film Institute",
    "Masters of Cinema", "Eureka Entertainment", "Eureka",
    "Indicator", "Indicator Series", "Powerhouse Films", "Powerhouse Indicator",
    "Second Sight", "Second Sight Films",
    "88 Films", "88 Asia",
    "Radiance Films", "Radiance",
    "101 Films",
    "Studiocanal", "Studio Canal",
    "Network Distributing", "Network",
    "Fabulous Films",
    "Signal One", "Signal One Entertainment",

    # US boutique labels
    "Blue Underground",
    "Severin Films", "Severin",
    "Synapse Films", "Synapse",
    "Grindhouse Releasing",
    "Code Red", "Code Red DVD",
    "Unearthed Films", "Unearthed Classics",
    "AGFA", "American Genre Film Archive",
    "Cult Epics",
    "CAV", "CAV Distributing",
    "Dark Force Entertainment",
    "Full Moon Features", "Full Moon",
    "Massacre Video",
    "Saturn's Core",
    "Terror Vision",
    "Visual Vengeance",
    "Intervision", "Intervision Picture Corp",
    "Mondo Macabro",
    "Mondo",
    "Raro Video",
    "Camera Obscura",
    "Altered Innocence",
    "Cinelicious Pics",
    "Dekanalog",
    "DiabolikDVD", "Diabolik",
    "Distribution Solutions",
    "Distribpix",
    "Garagehouse Pictures",
    "Gold Ninja Video",
    "Hemlock Films",
    "JVTVX",
    "Kitten Media",
    "MVD", "MVD Rewind", "MVD Visual", "MVD Entertainment",
    "Olive Films",
    "Oscilloscope", "Oscilloscope Laboratories",
    "Scorpion Releasing",
    "Shudder", "Shudder Exclusive",
    "Utopia Distribution",
    "Vinegar Syndrome Labs",
    "Wild Eye Releasing",

    # Premium/Collector labels
    "Twilight Time",
    "Fun City Editions",
    "Arbelos", "Arbelos Films",
    "Deaf Crocodile",
    "Le Chat Qui Fume",
    "Imprint", "Imprint Films", "Via Vision",
    "Explosive Media",
    "Wicked Vision",
    "Nameless Media",
    "NSM Records",
    "OFDb Filmworks",
    "Subkultur", "Subkultur Entertainment",
    "Anolis Entertainment",
    "Camera Obscura Mediabook",
    "Capelight Pictures",
    "Filmconfect",
    "Koch Media", "Koch Films",
    "Plaion Pictures",
    "Turbine Media", "Turbine Medien",

    # Classic/Archive labels
    "Warner Archive", "Warner Archive Collection", "WAC",
    "Cohen Film Collection", "Cohen Media Group",
    "Film Movement", "Film Movement Classics",
    "Flicker Alley",
    "Milestone Films", "Milestone",
    "Music Box Films",
    "Drafthouse Films",
    "MUBI",
    "Janus Films",
    "Grasshopper Film",
    "Icarus Films",
    "Kino Marquee",
    "Magnolia Pictures",
    "Metrograph Pictures",
    "NEON",
    "Photon Films",
    "Strand Releasing",

    # International boutique
    "Carlotta Films",
    "Gaumont",
    "Pathe",
    "Wild Side Video",
    "Elephant Films",
    "ESC Editions",
    "Rimini Editions",
    "Sidonis Calysta",
    "Spectrum Films",
    "CG Entertainment",
    "Midnight Factory",
    "Plaion", "Plaion Pictures",
    "Entertainment One", "eOne",
    "Umbrella Entertainment",
    "Madman Entertainment", "Madman",
    "Beyond Home Entertainment",
    "Shock Entertainment",
]

# Edition keywords that indicate special editions
EDITION_KEYWORDS = [
    "criterion", "collector", "collector's", "collectors",
    "limited edition", "limited", "special edition",
    "steelbook", "steel book",
    "director's cut", "directors cut",
    "anniversary", "restored", "remastered", "remaster",
    "ultimate edition", "deluxe", "premium",
    "box set", "boxset", "complete series", "complete collection",
    "slipcover", "slip cover", "mediabook",
    "digipack", "digipak",
    "booklet", "with booklet",
    "arrow exclusive", "shout exclusive",
]

# Keywords that indicate standard/non-special editions or unwanted formats
EXCLUDE_KEYWORDS = [
    # DVD format - we only want Blu-ray and 4K
    "dvd",
    # Standard editions
    "standard edition", "regular edition",
    # Retail exclusives (usually just different cover)
    "walmart exclusive",
    "target exclusive",
    "best buy exclusive",
    # Digital/streaming
    "digital code", "digital copy", "digital download",
    "streaming", "digital only",
    # Used/rental
    "rental", "previously viewed", "used", "pre-owned",
    "ex-rental", "ex rental",
    # Obsolete formats
    "vhs", "videotape", "laserdisc", "hd dvd", "hd-dvd",
    # Bootlegs/unauthorized
    "region free only", "all region", "bootleg",
    "unauthorized", "import copy",
]

# Format patterns
FORMAT_PATTERNS = {
    "4K UHD": [r"4k\s*u?h?d?", r"ultra\s*hd", r"4k\s*blu-?ray", r"uhd"],
    "Blu-ray": [r"blu-?ray", r"bluray", r"bd"],
    "DVD": [r"\bdvd\b"],
}


@dataclass
class ClassificationResult:
    """Result of edition classification."""
    is_special_edition: bool
    confidence: float
    format: str  # "4K UHD", "Blu-ray", "DVD", "Unknown"
    label: Optional[str]  # Boutique label if identified
    edition_type: Optional[str]  # "Collector's", "Limited", etc.
    reason: str  # Explanation for the classification


class EditionClassifier:
    """
    Rule-based classifier for movie product listings.
    Determines if a product is a special/collector's edition worth tracking.
    """

    def __init__(self):
        # Pre-compile regex patterns for efficiency
        self._label_patterns = self._compile_label_patterns()
        self._edition_patterns = self._compile_patterns(EDITION_KEYWORDS)
        self._exclude_patterns = self._compile_patterns(EXCLUDE_KEYWORDS)
        self._format_patterns = {
            fmt: [re.compile(p, re.IGNORECASE) for p in patterns]
            for fmt, patterns in FORMAT_PATTERNS.items()
        }

    def _compile_label_patterns(self) -> List[Tuple[str, re.Pattern]]:
        """Compile boutique label patterns."""
        patterns = []
        for label in BOUTIQUE_LABELS:
            # Create pattern that matches the label as a whole word
            pattern = re.compile(r'\b' + re.escape(label) + r'\b', re.IGNORECASE)
            patterns.append((label, pattern))
        return patterns

    def _compile_patterns(self, keywords: List[str]) -> List[re.Pattern]:
        """Compile keyword patterns."""
        return [
            re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
            for kw in keywords
        ]

    def _detect_format(self, title: str) -> str:
        """Detect the media format from the title."""
        title_lower = title.lower()

        # Check 4K first (highest priority)
        for pattern in self._format_patterns["4K UHD"]:
            if pattern.search(title_lower):
                return "4K UHD"

        # Check Blu-ray
        for pattern in self._format_patterns["Blu-ray"]:
            if pattern.search(title_lower):
                return "Blu-ray"

        # Check DVD
        for pattern in self._format_patterns["DVD"]:
            if pattern.search(title_lower):
                return "DVD"

        return "Unknown"

    def _find_boutique_label(self, title: str) -> Optional[str]:
        """Find a boutique label in the title."""
        for label, pattern in self._label_patterns:
            if pattern.search(title):
                return label
        return None

    def _find_edition_keywords(self, title: str) -> List[str]:
        """Find edition keywords in the title."""
        found = []
        for pattern in self._edition_patterns:
            if pattern.search(title):
                found.append(pattern.pattern.replace(r'\b', '').replace('\\', ''))
        return found

    def _is_excluded(self, title: str) -> Tuple[bool, Optional[str]]:
        """Check if the title contains exclusion keywords."""
        for pattern in self._exclude_patterns:
            if pattern.search(title):
                keyword = pattern.pattern.replace(r'\b', '').replace('\\', '')
                return True, keyword
        return False, None

    def classify(self, product_title: str) -> ClassificationResult:
        """
        Classify a product title.
        Returns ClassificationResult with details about the edition.
        """
        # Check for exclusions first
        is_excluded, exclude_reason = self._is_excluded(product_title)
        if is_excluded:
            return ClassificationResult(
                is_special_edition=False,
                confidence=0.9,
                format=self._detect_format(product_title),
                label=None,
                edition_type=None,
                reason=f"Excluded: contains '{exclude_reason}'"
            )

        # Detect format
        media_format = self._detect_format(product_title)

        # Find boutique label
        label = self._find_boutique_label(product_title)

        # Find edition keywords
        edition_keywords = self._find_edition_keywords(product_title)

        # Determine if it's a special edition
        is_special = False
        confidence = 0.0
        edition_type = None
        reason = ""

        if label:
            # Boutique label found - high confidence
            is_special = True
            confidence = 0.95
            edition_type = f"{label} Release"
            reason = f"Boutique label: {label}"
        elif edition_keywords:
            # Edition keywords found - medium-high confidence
            is_special = True
            confidence = 0.8
            edition_type = edition_keywords[0].title()
            reason = f"Edition keywords: {', '.join(edition_keywords[:3])}"
        else:
            # No special indicators
            is_special = False
            confidence = 0.7
            reason = "No boutique label or special edition indicators found"

        return ClassificationResult(
            is_special_edition=is_special,
            confidence=confidence,
            format=media_format,
            label=label,
            edition_type=edition_type,
            reason=reason
        )

    def is_special_edition(self, product_title: str) -> Tuple[bool, float, str]:
        """
        Compatibility method matching EditionMatcher interface.
        Returns (is_match, confidence_score, description).
        """
        result = self.classify(product_title)

        description = result.reason
        if result.label:
            description = f"{result.label} - {result.edition_type or 'Special Edition'}"
        elif result.edition_type:
            description = result.edition_type

        return (result.is_special_edition, result.confidence, description)


if __name__ == "__main__":
    # Test the classifier
    logging.basicConfig(level=logging.INFO)

    test_products = [
        "The Shining (Criterion Collection) [4K UHD Blu-ray]",
        "Jaws - Standard Blu-ray",
        "Alien 4K Ultra HD Steelbook Limited Edition",
        "Spider-Man DVD Walmart Exclusive",
        "Seven Samurai (Criterion Collection) Blu-ray",
        "Arrow Video: Society Limited Edition Blu-ray with Slipcover",
        "The Matrix - Regular DVD",
        "House (1977) Blu-ray Criterion Collection",
        "Suspiria 4K UHD Synapse Films",
        "Akira Limited Edition Steelbook 4K",
        "Office Space DVD",
        "Vertigo Blu-ray",
    ]

    classifier = EditionClassifier()

    print("\nRule-Based Classification Results:\n")
    print("-" * 70)

    for product in test_products:
        result = classifier.classify(product)
        status = "SPECIAL" if result.is_special_edition else "standard"
        print(f"\n[{status:8}] {result.confidence:.0%} | {product}")
        print(f"           Format: {result.format}, Label: {result.label or 'N/A'}")
        print(f"           Reason: {result.reason}")
