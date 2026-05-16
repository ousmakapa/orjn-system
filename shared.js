const STORAGE_KEY = "pageMonitors";

export const DEFAULT_INTERVAL_MINUTES = 1440;
export const SHOES_TYPE_METAFIELD_ENABLED_KEY = "shoesTypeMetafieldEnabledNames";
export const SHOES_TYPE_METAFIELD_DISABLED_KEY = "shoesTypeMetafieldDisabledNames";

const CANONICAL_BRAND_RULES = [
  { canonical: "Nike", patterns: [/^nike$/i, /^nike\s+sportswear$/i, /^nike\s+sb$/i, /^nike\s+\w/i] },
  { canonical: "Jordan", patterns: [/^air\s*jordan$/i, /^jordan$/i, /^jordan\s+brand$/i] },
  { canonical: "Adidas", patterns: [/^adidas$/i, /^adidas\s+originals$/i, /^adidas\s+sportswear$/i, /^adidas\s+\w/i] },
  { canonical: "Puma", patterns: [/^puma$/i, /^puma\s+sportswear$/i] },
  { canonical: "Reebok", patterns: [/^reebok$/i, /^reebok\s+classic$/i] },
  { canonical: "New Balance", patterns: [/^new\s*balance$/i, /^newbalance$/i, /^nb$/i] },
  { canonical: "Converse", patterns: [/^converse$/i, /^converse\s+all\s*star$/i, /^converse\s+cons$/i] },
  { canonical: "Vans", patterns: [/^vans$/i] },
  { canonical: "Under Armour", patterns: [/^under\s*armou?r$/i, /^ua$/i] },
  { canonical: "Asics", patterns: [/^asics$/i, /^asics\s+tiger$/i] },
  { canonical: "Saucony", patterns: [/^saucony$/i] },
  { canonical: "Brooks", patterns: [/^brooks$/i, /^brooks\s+running$/i] },
  { canonical: "Hoka", patterns: [/^hoka$/i, /^hoka\s+one\s+one$/i, /^hokaoneone$/i] },
  { canonical: "ON Cloud", patterns: [/^on$/i, /^on\s*running$/i, /^on\s*cloud$/i, /^on\s*cloud\s*running$/i] },
  { canonical: "Salomon", patterns: [/^salomon$/i, /^salomon\s+sportstyle$/i] },
  { canonical: "Timberland", patterns: [/^timberland$/i, /^timberland\s+pro$/i] },
  { canonical: "UGG", patterns: [/^ugg$/i, /^ugg\s+australia$/i] },
  { canonical: "Dr. Martens", patterns: [/^dr\.?\s*martens$/i, /^doc\s*martens$/i, /^drmartens$/i] },
  { canonical: "Birkenstock", patterns: [/^birkenstock$/i] },
  { canonical: "Clarks", patterns: [/^clarks$/i, /^clarks\s+originals$/i] },
  { canonical: "The North Face", patterns: [/^the\s*north\s*face$/i, /^north\s*face$/i, /^tnf$/i] },
  { canonical: "Columbia", patterns: [/^columbia$/i, /^columbia\s+sportswear$/i] },
  { canonical: "Patagonia", patterns: [/^patagonia$/i] },
  { canonical: "Supreme", patterns: [/^supreme$/i] },
  { canonical: "Off-White", patterns: [/^off[\s-]*white$/i, /^offwhite$/i] },
  { canonical: "Balenciaga", patterns: [/^balenciaga$/i] },
  { canonical: "Gucci", patterns: [/^gucci$/i] },
  { canonical: "Louis Vuitton", patterns: [/^louis\s*vuitton$/i, /^lv$/i] },
  { canonical: "Yeezy", patterns: [/^yeezy$/i, /^adidas\s+yeezy$/i] },
  { canonical: "Fila", patterns: [/^fila$/i] },
  { canonical: "Tommy Hilfiger", patterns: [/^tommy\s*hilfiger$/i, /^tommy$/i] },
  { canonical: "Ralph Lauren", patterns: [/^ralph\s*lauren$/i, /^polo\s*ralph\s*lauren$/i, /^polo$/i] },
  { canonical: "Lacoste", patterns: [/^lacoste$/i] },
  { canonical: "Champion", patterns: [/^champion$/i] },
  { canonical: "Kappa", patterns: [/^kappa$/i] },
  { canonical: "Umbro", patterns: [/^umbro$/i] },
  { canonical: "Ellesse", patterns: [/^ellesse$/i] },
  { canonical: "Diadora", patterns: [/^diadora$/i] },
  { canonical: "Le Coq Sportif", patterns: [/^le\s*coq\s*sportif$/i, /^lecoqsportif$/i] },
  { canonical: "Mizuno", patterns: [/^mizuno$/i] },
  { canonical: "Karhu", patterns: [/^karhu$/i] },
  { canonical: "Crocs", patterns: [/^crocs$/i] },
  { canonical: "Skechers", patterns: [/^skechers$/i, /^skecher$/i] },
  { canonical: "Steve Madden", patterns: [/^steve\s*madden$/i] },
  { canonical: "Ecco", patterns: [/^ecco$/i] },
  { canonical: "Geox", patterns: [/^geox$/i] },
  { canonical: "Camper", patterns: [/^camper$/i] },
  { canonical: "Stussy", patterns: [/^stussy$/i, /^st[uü]ssy$/i] },
  { canonical: "Palace", patterns: [/^palace$/i, /^palace\s+skateboards$/i] },
  { canonical: "Kith", patterns: [/^kith$/i] },
  { canonical: "Carhartt", patterns: [/^carhartt$/i, /^carhartt\s+wip$/i] },
  { canonical: "Dickies", patterns: [/^dickies$/i] },
  { canonical: "Stone Island", patterns: [/^stone\s*island$/i] },
  { canonical: "Moncler", patterns: [/^moncler$/i] },
  { canonical: "Arc'teryx", patterns: [/^arc[\s'’]*teryx$/i] },
  { canonical: "Merrell", patterns: [/^merrell$/i] },
  { canonical: "Keen", patterns: [/^keen$/i] },
  { canonical: "Teva", patterns: [/^teva$/i] },
  { canonical: "Calvin Klein", patterns: [/^calvin\s*klein$/i, /^ck$/i] },
  { canonical: "Hugo Boss", patterns: [/^hugo\s*boss$/i, /^boss$/i, /^boss\s+by\s+hugo\s+boss$/i] },
  { canonical: "Way of Wade", patterns: [/^way\s+of\s+wade$/i, /^li[\s-]*ning$/i, /^lining$/i] }
];

export function uid(prefix = "monitor") {
  const random = Math.random().toString(36).slice(2, 10);
  return `${prefix}_${Date.now()}_${random}`;
}

export function normalizeText(value) {
  return (value ?? "")
    .replace(/\s+/g, " ")
    .replace(/\u00a0/g, " ")
    .trim();
}

function hasRequiredMonitorText(value) {
  const text = normalizeText(value);
  return !!text && !/^(?:not defined|unknown|n\/a|null|undefined)$/i.test(text);
}

export function getMissingProductDataFields(monitor = {}) {
  if (monitor.pendingInitialCheck) return [];
  const pd = monitor.productData || {};
  const live = monitor.lastExtractedData || {};
  const missing = [];
  const hasColor = [pd.colorFinal, pd.color, pd.colorRaw].some(hasRequiredMonitorText);
  const hasImages = Array.isArray(pd.images) && pd.images.some((src) => hasRequiredMonitorText(src));
  const hasPrice = live.price != null;
  const hasSizes =
    (Array.isArray(live.inStock) && live.inStock.length > 0) ||
    (Array.isArray(live.outOfStock) && live.outOfStock.length > 0);

  if (!monitor.productData) missing.push("product data");
  if (!hasRequiredMonitorText(pd.name)) missing.push("name");
  if (!hasRequiredMonitorText(pd.brand)) missing.push("brand");
  if (!hasRequiredMonitorText(pd.type)) missing.push("type");
  if (!hasRequiredMonitorText(pd.gender) && !hasRequiredMonitorText(pd.genderDisplay) && !hasRequiredMonitorText(pd.extractedGender)) missing.push("gender");
  if (!hasColor) missing.push("color");
  if (!hasRequiredMonitorText(pd.sku)) missing.push("SKU");
  if (!hasRequiredMonitorText(pd.description)) missing.push("description");
  if (!hasImages) missing.push("pictures");
  if (!hasPrice) missing.push("price");
  if (!hasSizes) missing.push("sizes");
  return missing;
}

export function withIncompleteDataError(monitor = {}) {
  if (monitor.pendingInitialCheck) return monitor;
  const missing = getMissingProductDataFields(monitor);
  if (!missing.length) return monitor;
  const lastError = `Missing product data: ${missing.join(", ")}`;
  if (monitor.status === "error" && monitor.lastError === lastError) return monitor;
  return { ...monitor, status: "error", lastError };
}

export function canonicalizeBrand(value) {
  const normalized = normalizeText(value);
  if (!normalized) return "";
  for (const rule of CANONICAL_BRAND_RULES) {
    if (rule.patterns.some((pattern) => pattern.test(normalized))) {
      return rule.canonical;
    }
  }
  return normalized;
}

function isDicksSportingGoodsMonitor(monitor) {
  try {
    return /(^|\.)dickssportinggoods\.com\b/i.test(new URL(monitor?.url || "").hostname || "");
  } catch (_) {
    return /dickssportinggoods\.com/i.test(String(monitor?.url || ""));
  }
}

export function removeDsgSoccerCleatsText(value, { allowEmpty = false } = {}) {
  if (typeof value !== "string") return value;
  const cleaned = value
    .replace(/\bsoccer\s+cleats?\b/gi, "")
    .replace(/\s+([|,/])/g, "$1")
    .replace(/([|,/])\s+/g, "$1 ")
    .replace(/\s*[-–—]\s*([-–—]\s*)+/g, " - ")
    .replace(/^\s*[-–—|,/]+\s*/, "")
    .replace(/\s*[-–—|,/]+\s*$/, "")
    .replace(/\(\s*\)/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();
  return cleaned || (allowEmpty ? "" : value);
}

export function normalizeDsgSoccerCleatsMonitor(monitor) {
  if (!monitor || typeof monitor !== "object" || !isDicksSportingGoodsMonitor(monitor)) return monitor;
  let changed = false;
  const next = { ...monitor };

  const cleanRequired = (value) => removeDsgSoccerCleatsText(value, { allowEmpty: false });
  const cleanOptional = (value) => {
    const cleaned = removeDsgSoccerCleatsText(value, { allowEmpty: true });
    return cleaned || null;
  };

  const cleanObject = (object) => {
    if (!object || typeof object !== "object") return object;
    let objectChanged = false;
    const copy = { ...object };
    for (const field of ["name", "title"]) {
      if (typeof copy[field] === "string") {
        const cleaned = cleanRequired(copy[field]);
        if (cleaned !== copy[field]) {
          copy[field] = cleaned;
          objectChanged = true;
        }
      }
    }
    if (typeof copy.type === "string") {
      const cleanedType = cleanOptional(copy.type);
      if (cleanedType !== copy.type) {
        copy.type = cleanedType;
        objectChanged = true;
      }
    }
    return objectChanged ? copy : object;
  };

  if (typeof next.name === "string") {
    const cleanedName = cleanRequired(next.name);
    if (cleanedName !== next.name) {
      next.name = cleanedName;
      changed = true;
    }
  }

  const productData = cleanObject(next.productData);
  if (productData !== next.productData) {
    next.productData = productData;
    changed = true;
  }

  const overrides = cleanObject(next.productDataOverrides);
  if (overrides !== next.productDataOverrides) {
    next.productDataOverrides = overrides;
    changed = true;
  }

  return changed ? next : monitor;
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function removeRepeatedLeadingBrand(name, brand) {
  if (typeof name !== "string" || typeof brand !== "string") return name;
  const cleanBrand = brand.replace(/\s+/g, " ").trim();
  if (!cleanBrand) return name;
  const brandPattern = escapeRegExp(cleanBrand).replace(/\s+/g, "\\s+");
  const repeatedBrand = new RegExp(`^\\s*(${brandPattern})(?:\\s+|[-_/]+)${brandPattern}\\b`, "i");
  const cleaned = name.replace(repeatedBrand, "$1").replace(/\s{2,}/g, " ").trim();
  return cleaned || name;
}

function titleCaseShoeType(value) {
  return String(value || "")
    .split(" ")
    .filter(Boolean)
    .map((part) => {
      if (/^\d/.test(part)) return part.toUpperCase();
      if (/^(?:ii|iii|iv|v|vi|vii|viii|ix|x)$/i.test(part)) return part.toUpperCase();
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");
}

function removeBrandFromName(name, brand) {
  let text = removeRepeatedLeadingBrand(name, brand);
  const cleanBrand = normalizeText(brand);
  if (!cleanBrand) return text;
  if (/^jordan$/i.test(cleanBrand)) {
    // Only strip leading "Jordan" — global replace would destroy "Air Jordan" model names
    text = text.replace(/^\s*Jordan\b(?:\s+|[-_/]+)?/i, "");
    return normalizeText(text);
  }
  const brandAliases = /^on\s+cloud$/i.test(cleanBrand) ? ["On"] : [cleanBrand];
  for (const brandAlias of brandAliases) {
    const brandPattern = escapeRegExp(brandAlias).replace(/\s+/g, "\\s+");
    text = text.replace(new RegExp(`^\\s*${brandPattern}\\b(?:\\s+|[-_/]+)?`, "i"), "");
    text = text.replace(new RegExp(`\\b${brandPattern}\\b`, "gi"), " ");
  }
  return normalizeText(text);
}

export function deriveShoesType(productData = {}) {
  const productType = normalizeText(productData.type || "");
  if (!/^(?:lifestyle|running|training|basketball|football)$/i.test(productType)) return "";

  const brand = canonicalizeBrand(productData.brand || "");
  let name = normalizeText(productData.name || productData.title || "");
  if (!name) return "";

  name = name.replace(/\bjordan\s+retro\s+(\d+)\b/gi, "Air Jordan $1");
  name = removeBrandFromName(name, brand)
    .replace(/[()]/g, " ")
    .replace(/['’]/g, "")
    .replace(/\baf[\s-]*1\b/gi, "Air Force 1")
    .replace(/\bairforce\b/gi, "Air Force")
    .replace(/\s+/g, " ")
    .trim();
  if (!name) return "";

  // Normalize Jordan shorthand before any type-specific matching
  name = name
    .replace(/\bAJ\s+Retro\s+(\d+)\b/gi, "Air Jordan $1")
    .replace(/\bAJ\s*(\d+)\b/gi, "Air Jordan $1")
    .replace(/\bAir\s+Jordan\s+Retro\s+(\d+)\b/gi, "Air Jordan $1");
  if (!name) return "";

  const lowerName = name.toLowerCase();
  let result = "";

  if (/^basketball$/i.test(productType)) {
    const playerPatterns = [
      // Nike
      [/\bkd\d*\b|\bkevin\s+durant\b/, "Kevin Durant"],
      [/\blebron\b|\blbj\b/, "LeBron James"],
      [/\bkobe\b|\bkb\s*\d/, "Kobe Bryant"],
      [/\bkyrie\b/, "Kyrie Irving"],
      [/\bpg\s*\d|\bpaul\s+george\b/, "Paul George"],
      [/\bzoom\s+freak\b|\bfreak\b|\bgiannis\b/, "Giannis Antetokounmpo"],
      [/\bzion\b/, "Zion Williamson"],
      [/\bja\s+\d|\bja\s+morant\b/, "Ja Morant"],
      [/\bbook\s*\d|\bbooker\b/, "Devin Booker"],
      [/\bsabrina\b/, "Sabrina Ionescu"],
      [/\bwembanyama\b|\bwemby\b/, "Victor Wembanyama"],
      [/\bpenny\b/, "Penny Hardaway"],
      // Jordan Brand — no player association, falls to "Basketball" fallback
      // (Air Jordan basketball shoes → "Jordan Basketball" via brand prepend)
      [/\bluka\b/, "Luka Doncic"],
      [/\btatum\b/, "Jayson Tatum"],
      [/\bcarmelo\b|\bmelo\s*\d/, "Carmelo Anthony"],
      // Adidas
      [/\bharden\b/, "James Harden"],
      [/\bdame\s*\d|\bdamian\s+lillard\b/, "Damian Lillard"],
      [/\btrae\b/, "Trae Young"],
      [/\bae\s*\d|\banthony\s+edwards\b/, "Anthony Edwards"],
      [/\bd\.o\.n\b|\bdon\s+issue|\bdonovan\s+mitchell\b/, "Donovan Mitchell"],
      [/\bd\s*rose\s*\d|\bderrick\s+rose\b/, "Derrick Rose"],
      // Under Armour
      [/\bcurry\b/, "Stephen Curry"],
      [/\bembiid\b/, "Joel Embiid"],
      // Puma
      [/\blamelo\b|\bmb[\s.]*\d/, "LaMelo Ball"],
      [/\bscoot\b/, "Scoot Henderson"],
      [/\bstewie\b/, "Breanna Stewart"],
      // New Balance
      [/\bkawhi\b/, "Kawhi Leonard"],
      // Anta
      [/\bklay\b/, "Klay Thompson"],
      // Reebok
      [/\biverson\b|\bthe\s+question\b|\bquestion\s*\d/, "Allen Iverson"],
      [/\bangel\s+reese\b|\breese\s*\d/i, "Angel Reese"],
      // Way of Wade
      [/\bd['']?lo\s*\d/i, "D'Angelo Russell"],
      // Converse
      [/\bshai\b|\bshai\s+0+\d/, "Shai Gilgeous-Alexander"],
    ];
    const playerMatch = playerPatterns.find(([p]) => p.test(lowerName));
    if (playerMatch) {
      const cat = brand ? `${brand} Basketball` : "Basketball";
      return { category: cat, model: playerMatch[1] };
    }
    result = brand ? `${brand} Basketball` : "Basketball";
  } else if (/^football$/i.test(productType)) {
    const n = ` ${name} `;
    const surfaces = new Set();
    if (/\bMXSG\b/.test(n)) { surfaces.add('Molded'); surfaces.add('Soft Ground'); }
    if (/\bFG\/MG\b/.test(n)) { surfaces.add('Firm Ground'); surfaces.add('Molded'); }
    if (/\bFG\/AG\b/.test(n)) { surfaces.add('Firm Ground'); surfaces.add('Artificial Grass'); }
    if (/\bAG[-\s]?Pro\b/i.test(n)) surfaces.add('Artificial Grass');
    if (/\bFxG\b/i.test(n)) surfaces.add('Firm Ground');
    if (!surfaces.size) {
      if (/\bFG\b/.test(n)) surfaces.add('Firm Ground');
      if (/\bMG\b/.test(n)) surfaces.add('Molded');
      if (/\bAG\b/.test(n)) surfaces.add('Artificial Grass');
      if (/\bSG\b/.test(n)) surfaces.add('Soft Ground');
      if (/\bHG\b/.test(n)) surfaces.add('Hard Ground');
    }
    if (/\bTurf\b/i.test(n)) surfaces.add('Turf');
    if (/\bIndoor\b|\bSala\b|\b\bIC\b/i.test(n)) surfaces.add('Indoor');
    const cat = brand ? `${brand} Football` : "Football";
    const model = surfaces.size ? [...surfaces].join(', ') : 'Unknown';
    return { category: cat, model };
  } else if (/^training$/i.test(productType)) {
    const trainingPatterns = [
      // Nike — more specific variants before generic
      [/^mc\s+trainer\b/, "MC Trainer"],
      [/^alpha\s+trainer\b/, "Alpha Trainer"],
      [/^romal[e]?os\b|^romel[e]?os\b/, "Romaleos"],
      [/^air\s+zoom\s+superrep\b/, "SuperRep"],
      [/^zoom\s+superrep\b/, "SuperRep"],
      [/^superrep\b/, "SuperRep"],
      [/^react\s+metcon\b/, "Metcon"],
      [/^free\s+metcon\b/, "Metcon"],
      [/^metcon\b/, "Metcon"],
      [/^air\s+zoom\s+tr\b/, "Zoom TR"],
      [/^zoom\s+tr\b/, "Zoom TR"],
      // Adidas
      [/^dropset\b/, "Dropset"],
      [/^powerlift\b/, "Powerlift"],
      [/^adipower\b/, "Adipower"],
      // Under Armour
      [/^project\s+rock\b/, "Project Rock"],
      [/^hovr\s+rise\b/, "HOVR Rise"],
      [/^charged\s+commit\b/, "Charged Commit"],
      [/^charged\s+engage\b/, "Charged Engage"],
      [/^tribase\b/, "Tribase"],
      // Puma
      [/^fuse\b/, "Fuse"],
      // Reebok
      [/^legacy\s+lifter\b/, "Legacy Lifter"],
      [/^lifter\b/, "Lifter"],
      [/^nano\b/, "Nano"],
      [/^speed\s+tr\b/, "Speed TR"],
      // New Balance
      [/^minimus\b/, "Minimus"],
    ];
    const trainingMatch =
      trainingPatterns.find(([p]) => p.test(lowerName)) ??
      trainingPatterns.find(([p]) => new RegExp(p.source.replace(/^\^/, "\\b"), "").test(lowerName));
    if (trainingMatch) {
      result = trainingMatch[1];
    } else {
      const stopWords = new Set([
        "mens", "men", "womens", "women", "kids", "youth", "big", "little", "unisex",
        "shoes", "shoe", "sneakers", "sneaker", "trainer", "trainers", "training", "lifestyle"
      ]);
      const trailingModifiers = new Set([
        "low", "lo", "high", "hi", "mid", "og", "retro", "premium", "prm", "se", "sp", "qs",
        "lv8", "easyon", "easy", "essential", "essentials", "platform", "utility", "leather",
        "suede", "mesh", "flyknit", "flyease", "goretex", "gore-tex", "gtx", "wide"
      ]);
      const tokens = lowerName
        .split(/[^a-z0-9.+-]+/i)
        .map((t) => t.trim())
        .filter(Boolean)
        .filter((t) => !stopWords.has(t));
      while (tokens.length > 1 && trailingModifiers.has(tokens[tokens.length - 1])) {
        tokens.pop();
      }
      result = titleCaseShoeType(tokens.join(" "));
    }
  } else {
    // lifestyle / running

    // Dynamic: Air Jordan <number> — anchored first, then unanchored for prefixed names
    const airJordanNumMatch =
      /^air\s+jordan\s+(\d+)\b/.exec(lowerName) ??
      /\bair\s+jordan\s+(\d+)\b/.exec(lowerName);
    if (airJordanNumMatch) {
      result = `Air Jordan ${airJordanNumMatch[1]}`;
    }

    if (!result) {
    const familyPatterns = [
      [/^react\s+metcon\b/, "Metcon"],
      [/^free\s+metcon\b/, "Metcon"],
      [/^metcon\b/, "Metcon"],
      [/^astrograbber\b/, "Astrograbber"],
      [/^air\s+vapormax\b/, "VaporMax"],
      [/^vapormax\b/, "VaporMax"],
      [/^ava\s+rover\b/, "Ava Rover"],
      [/^sb\s+dunk\b/, "SB Dunk"],
      [/^dunk\b/, "Dunk"],
      [/^air\s+jordan\s+skyline\b/, "Air Jordan Skyline"],
      [/^air\s+jordan\b/, "Air Jordan"],
      [/^jordan\s+retro\b/, "Jordan Retro"],
      [/^spizike\b/, "Spizike"],
      [/^6\s+rings\b/, "Air Jordan 6"],
      [/^flight\s+court\b/, "Flight Court"],
      [/^jumpman\s+pro\b/, "Jumpman Pro"],
      [/^jumpman\b/, "Jumpman"],
      [/^mvp\b/, "MVP"],
      [/^jordan\b/, "Jordan"],
      [/^blazer\b/, "Blazer"],
      [/^cortez\b/, "Cortez"],
      [/^p[-\s]*6000\b/, "P-6000"],
      [/^v2k\s+run\b/, "V2K Run"],
      [/^waffle\b/, "Waffle"],
      [/^killshot\b/, "Killshot"],
      [/^field\s+general\b/, "Field General"],
      [/^shox\b/, "Shox"],
      [/^air\s+rift\b/, "Air Rift"],
      [/^ld[-\s]*1000\b/, "LD-1000"],
      [/^zoom\s+pegasus\b/, "Pegasus"],
      [/^pegasus\b/, "Pegasus"],
      [/^vomero\b/, "Vomero"],
      [/^alphafly\b/, "Alphafly"],
      [/^vaporfly\b/, "Vaporfly"],
      [/^invincible\b/, "Invincible"],
      [/^structure\b/, "Structure"],
      [/^winflo\b/, "Winflo"],
      [/^reactx?\s+infinity\b/, "React Infinity"],
      [/^infinity\s+run\b/, "Infinity Run"],
      [/^free\s+run\b/, "Free Run"],
      [/^free\b/, "Free"],
      [/^samba\b/, "Samba"],
      [/^gazelle\b/, "Gazelle"],
      [/^campus\b/, "Campus"],
      [/^superstar\b/, "Superstar"],
      [/^stan\s+smith\b/, "Stan Smith"],
      [/^forum\b/, "Forum"],
      [/^handball\s+spezial\b/, "Handball Spezial"],
      [/^spezial\b/, "Spezial"],
      [/^sl\s*72\b/, "SL 72"],
      [/^ozweego\b/, "Ozweego"],
      [/^retropy\b/, "Retropy"],
      [/^nmd\b/, "NMD"],
      [/^zx\b/, "ZX"],
      [/^supernova\b/, "Supernova"],
      [/^ultraboost\b/, "Ultraboost"],
      [/^adizero\s+adios\b/, "Adizero Adios"],
      [/^adizero\s+prime\b/, "Adizero Prime"],
      [/^adizero\b/, "Adizero"],
      [/^yeezy\s+boost\b/, "Yeezy Boost"],
      [/^yeezy\b/, "Yeezy"],
      [/^gel[-\s]+kayano\b/, "Gel-Kayano"],
      [/^gel[-\s]+cumulus\b/, "Gel-Cumulus"],
      [/^gel[-\s]+nyc\b/, "Gel-NYC"],
      [/^gel[-\s]+lyte\b/, "Gel-Lyte"],
      [/^gel[-\s]+quantum\b/, "Gel-Quantum"],
      [/^gt[-\s]+2160\b/, "GT-2160"],
      [/^gt[-\s]+2000\b/, "GT-2000"],
      [/^novablast\b/, "Novablast"],
      [/^kayano\b/, "Kayano"],
      [/^cloudmonster\b/, "Cloudmonster"],
      [/^cloudnova\b/, "Cloudnova"],
      [/^cloudrunner\b/, "Cloudrunner"],
      [/^cloudsurfer\b/, "Cloudsurfer"],
      [/^cloudswift\b/, "Cloudswift"],
      [/^cloudtilt\b/, "Cloudtilt"],
      [/^cloud\s*\d*\b/, "Cloud"],
      [/^speedgoat\b/, "Speedgoat"],
      [/^solimar\b/, "Solimar"],
      [/^mafate\b/, "Mafate"],
      [/^bondi\b/, "Bondi"],
      [/^clifton\b/, "Clifton"],
      [/^mach\b/, "Mach"],
      [/^transport\b/, "Transport"],
      [/^xt[-\s]*6\b/, "XT-6"],
      [/^acs\s+pro\b/, "ACS Pro"],
      [/^speedcross\b/, "Speedcross"],
      [/^suede\b/, "Suede"],
      [/^palermo\b/, "Palermo"],
      [/^speedcat\b/, "Speedcat"],
      [/^easy\s+rider\b/, "Easy Rider"],
      [/^rs[-\s]*x\b/, "RS-X"],
      [/^clyde\b/, "Clyde"],
      [/^990\b/, "990"],
      [/^991\b/, "991"],
      [/^992\b/, "992"],
      [/^993\b/, "993"],
      [/^1000\b/, "1000"],
      [/^9060\b/, "9060"],
      [/^2002r\b/, "2002R"],
      [/^1906r\b/, "1906R"],
      [/^196r?\b/, "196R"],
      [/^860\b/, "860"],
      [/^740\b/, "740"],
      [/^480\b/, "480"],
      [/^574\b/, "574"],
      [/^327\b/, "327"],
      [/^(?:bb)?550\b/, "550"],
      [/^530\b/, "530"],
      [/^fresh\s+foam\b/, "Fresh Foam"],
      [/^fuelcell\b/, "FuelCell"],
      [/^club\s+c\b/, "Club C"],
      [/^classic\s+leather\b/, "Classic Leather"],
      [/^instapump\s+fury\b/, "Instapump Fury"],
      [/^chuck\s+70\b/, "Chuck 70"],
      [/^chuck\s+taylor\b/, "Chuck Taylor"],
      [/^all\s+star\b/, "All Star"],
      [/^old\s+skool\b/, "Old Skool"],
      [/^sk8[-\s]+hi\b/, "Sk8-Hi"],
      [/^authentic\b/, "Authentic"]
    ];

    // First pass: model at start. Second pass: model anywhere (handles colorway/collab prefixes).
    const familyMatch =
      familyPatterns.find(([p]) => p.test(lowerName)) ??
      familyPatterns.find(([p]) => new RegExp(p.source.replace(/^\^/, "\\b"), "").test(lowerName));

    if (/\bair\s+force\s+1\b/.test(lowerName)) {
      result = "Air Force 1";
    } else if (/\bair\s+max\b/.test(lowerName)) {
      result = "Air Max";
    } else if (familyMatch) {
      result = familyMatch[1];
    } else {
      const stopWords = new Set([
        "mens", "men", "womens", "women", "kids", "youth", "big", "little", "unisex",
        "shoes", "shoe", "sneakers", "sneaker", "trainer", "trainers", "running", "lifestyle"
      ]);
      const trailingModifiers = new Set([
        "low", "lo", "high", "hi", "mid", "og", "retro", "premium", "prm", "se", "sp", "qs",
        "lv8", "easyon", "easy", "essential", "essentials", "platform", "utility", "leather",
        "suede", "mesh", "flyknit", "flyease", "goretex", "gore-tex", "gtx", "wide"
      ]);

      const tokens = lowerName
        .split(/[^a-z0-9.+-]+/i)
        .map((token) => token.trim())
        .filter(Boolean)
        .filter((token) => !stopWords.has(token));

      while (tokens.length > 1 && trailingModifiers.has(tokens[tokens.length - 1])) {
        tokens.pop();
      }

      result = titleCaseShoeType(tokens.join(" "));
    }
    } // end if (!result)
  }

  const typeLabel = /^basketball$/i.test(productType) ? "Basketball"
    : /^training$/i.test(productType) ? "Training"
    : /^running$/i.test(productType) && result !== "Air Max" ? "Running"
    : "Lifestyle";
  const category = brand ? `${brand} ${typeLabel}` : typeLabel;
  if (/^lifestyle$/i.test(productType) && /^new\s+balance$/i.test(brand) && result && !/^new\s+balance\b/i.test(result)) {
    result = `New Balance ${result}`;
  }

  return { category, model: result };
}

function normalizeMonitorBrandData(monitor) {
  if (!monitor || typeof monitor !== "object") return monitor;
  let changed = false;
  const next = { ...monitor };

  if (monitor.productData && typeof monitor.productData === "object") {
    const normalizedBrand = canonicalizeBrand(monitor.productData.brand);
    const productData = { ...monitor.productData };
    if (normalizedBrand && normalizedBrand !== productData.brand) {
      productData.brand = normalizedBrand;
      changed = true;
    }
    const brandForName = normalizedBrand || productData.brand || "";
    if (brandForName && typeof productData.name === "string") {
      const cleanedName = removeRepeatedLeadingBrand(productData.name, brandForName);
      if (cleanedName !== productData.name) {
        productData.name = cleanedName;
        changed = true;
      }
    }
    if (changed || productData !== monitor.productData) {
      next.productData = productData;
    }
  }

  if (next.productData?.brand && typeof next.name === "string") {
    const cleanedMonitorName = removeRepeatedLeadingBrand(next.name, next.productData.brand);
    if (cleanedMonitorName !== next.name) {
      next.name = cleanedMonitorName;
      changed = true;
    }
  }

  if (monitor.productDataOverrides && typeof monitor.productDataOverrides === "object") {
    const normalizedOverrideBrand = canonicalizeBrand(monitor.productDataOverrides.brand);
    const overrides = { ...monitor.productDataOverrides };
    if (normalizedOverrideBrand && normalizedOverrideBrand !== overrides.brand) {
      overrides.brand = normalizedOverrideBrand;
      changed = true;
    }
    const overrideBrandForName = normalizedOverrideBrand || overrides.brand || next.productData?.brand || "";
    if (overrideBrandForName && typeof overrides.name === "string") {
      const cleanedOverrideName = removeRepeatedLeadingBrand(overrides.name, overrideBrandForName);
      if (cleanedOverrideName !== overrides.name) {
        overrides.name = cleanedOverrideName;
        changed = true;
      }
    }
    if (changed || overrides !== monitor.productDataOverrides) {
      next.productDataOverrides = overrides;
    }
  }

  if (next.productData?.brand && typeof next.productData?.title === "string") {
    const cleanedTitle = removeRepeatedLeadingBrand(next.productData.title, next.productData.brand);
    if (cleanedTitle !== next.productData.title) {
      next.productData = { ...next.productData, title: cleanedTitle };
      changed = true;
    }
  }

  const brandNormalized = changed ? next : monitor;
  return normalizeDsgSoccerCleatsMonitor(brandNormalized);
}

function cleanSizeLabel(s) {
  return String(s).replace(/^Size:\s*/i, "").replace(/\s*[-–]\s*Sold\s+Out/i, "").trim();
}

function isWayOfWadeMonitor(monitor = {}) {
  const brand = `${monitor.productData?.brand || ""} ${monitor.productDataOverrides?.brand || ""} ${monitor.name || ""}`;
  if (/\b(?:way\s+of\s+wade|li[\s-]*ning|lining)\b/i.test(brand)) return true;
  try {
    return /(^|\.)wayofwade\.com\b/i.test(new URL(monitor.url || "").hostname || "");
  } catch (_) {
    return /wayofwade\.com/i.test(String(monitor.url || ""));
  }
}

function normalizeWadeUsMSize(value) {
  const n = Number(String(value || "").replace(",", "."));
  return Number.isFinite(n) && n > 0 && n <= 30 ? String(n) : "";
}

function extractWadeUsMSizeLabel(value) {
  const text = cleanSizeLabel(value).replace(/,/g, ".");
  const usM = text.match(/\bUS-?M\s*([0-9]+(?:\.\d+)?)/i) || text.match(/^([0-9]+(?:\.\d+)?)$/);
  return usM ? normalizeWadeUsMSize(usM[1]) : "";
}

function cleanSizeList(list, monitor = {}) {
  if (!Array.isArray(list)) return list;
  const wade = isWayOfWadeMonitor(monitor);
  const seen = new Set();
  const cleaned = list
    .map((size) => wade ? extractWadeUsMSizeLabel(size) : cleanSizeLabel(size))
    .filter(Boolean)
    .filter((size) => {
      const key = size.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  return cleaned.length !== list.length || cleaned.some((s, i) => s !== list[i]) ? cleaned : list;
}

function normalizeSizeLabels(monitor) {
  const live = monitor.lastExtractedData;
  let next = monitor;
  if (live) {
    const inStock = cleanSizeList(live.inStock, monitor);
    const outOfStock = cleanSizeList(live.outOfStock, monitor);
    if (inStock !== live.inStock || outOfStock !== live.outOfStock) {
      next = { ...next, lastExtractedData: { ...live, inStock, outOfStock } };
    }
  }
  if (monitor.productData && typeof monitor.productData === "object") {
    const sizes = cleanSizeList(monitor.productData.sizes, monitor);
    const outOfStock = cleanSizeList(monitor.productData.outOfStock, monitor);
    if (sizes !== monitor.productData.sizes || outOfStock !== monitor.productData.outOfStock) {
      next = {
        ...next,
        productData: {
          ...next.productData,
          ...(sizes !== monitor.productData.sizes ? { sizes } : {}),
          ...(outOfStock !== monitor.productData.outOfStock ? { outOfStock } : {})
        }
      };
    }
  }
  return next;
}
function normalizeMonitors(monitors) {
  let changed = false;
  const normalized = (Array.isArray(monitors) ? monitors : []).map((monitor) => {
    let next = normalizeMonitorBrandData(monitor);
    next = normalizeSizeLabels(next);
    if (next !== monitor) changed = true;
    return next;
  });
  return { changed, monitors: normalized };
}

export function buildSelector(element) {
  if (!element || element.nodeType !== Node.ELEMENT_NODE) {
    return "";
  }

  if (element.id) {
    return `#${CSS.escape(element.id)}`;
  }

  const parts = [];
  let current = element;

  while (current && current.nodeType === Node.ELEMENT_NODE && current !== document.body) {
    let part = current.localName;
    if (!part) {
      break;
    }

    const classNames = Array.from(current.classList).slice(0, 2);
    if (classNames.length) {
      part += classNames.map((name) => `.${CSS.escape(name)}`).join("");
    }

    const parent = current.parentElement;
    if (parent) {
      const siblings = Array.from(parent.children).filter((child) => child.localName === current.localName);
      if (siblings.length > 1) {
        const index = siblings.indexOf(current) + 1;
        part += `:nth-of-type(${index})`;
      }
    }

    parts.unshift(part);

    const selector = parts.join(" > ");
    try {
      const resolved = document.querySelector(selector);
      if (resolved === element) {
        return selector;
      }
    } catch (error) {
      // Keep walking up until the selector is valid and unique.
    }

    current = parent;
  }

  return parts.join(" > ");
}

export function extractSnapshot(doc, monitor) {
  if (!doc) {
    return {
      ok: false,
      summary: "",
      error: "Document unavailable"
    };
  }

  if (!monitor.selector) {
    return {
      ok: true,
      summary: normalizeText(doc.body?.innerText || doc.documentElement?.textContent || ""),
      matched: "full-page"
    };
  }

  try {
    const element = doc.querySelector(monitor.selector);
    if (!element) {
      return {
        ok: false,
        summary: "",
        error: "Selector not found"
      };
    }

    return {
      ok: true,
      summary: normalizeText(element.innerText || element.textContent || ""),
      matched: monitor.selector
    };
  } catch (error) {
    return {
      ok: false,
      summary: "",
      error: `Invalid selector: ${error.message}`
    };
  }
}

// ── Activity log ──────────────────────────────────────────────────────────
const LOGS_KEY = "monitorLogs";
const MAX_LOGS = 500;

export async function addLog(entry) {
  const { [LOGS_KEY]: logs = [] } = await chrome.storage.local.get(LOGS_KEY);
  logs.unshift({ id: uid("log"), timestamp: new Date().toISOString(), ...entry });
  if (logs.length > MAX_LOGS) logs.length = MAX_LOGS;
  await chrome.storage.local.set({ [LOGS_KEY]: logs });
}

export async function getLogs() {
  const { [LOGS_KEY]: logs = [] } = await chrome.storage.local.get(LOGS_KEY);
  return logs;
}

export async function clearLogs() {
  await chrome.storage.local.set({ [LOGS_KEY]: [] });
}

// ── Per-monitor storage (faster concurrent reads/writes) ──────────────────
// Each monitor lives under its own key: monitor:{id}
// The full pageMonitors array is kept as a permanent read-only backup and is
// NEVER modified after migration — it is purely a safety net.
// ── IndexedDB storage layer ───────────────────────────────────────────────
// Replaces chrome.storage.local for monitor data. No quota limit, 5-10× faster
// reads, and handles 10k+ monitors without issue. chrome.storage is kept only
// for non-monitor settings (Shopify tokens, UI state, etc.).

const IDB_NAME = "riad-distil";
const IDB_VERSION = 1;
const IDB_STORE = "monitors";

let _idbPromise = null;
function _openIdb() {
  if (_idbPromise) return _idbPromise;
  _idbPromise = new Promise((resolve, reject) => {
    const req = indexedDB.open(IDB_NAME, IDB_VERSION);
    req.onupgradeneeded = (e) => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains(IDB_STORE)) {
        db.createObjectStore(IDB_STORE, { keyPath: "id" });
      }
    };
    req.onsuccess = (e) => resolve(e.target.result);
    req.onerror = (e) => { _idbPromise = null; reject(e.target.error); };
  });
  return _idbPromise;
}

function _idbTx(mode, fn) {
  return _openIdb().then(db => new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, mode);
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error || new Error("IDB transaction aborted"));
    fn(tx.objectStore(IDB_STORE), resolve, reject);
  }));
}

function _idbGetOne(id) {
  return _idbTx("readonly", (store, resolve, reject) => {
    const req = store.get(id);
    req.onsuccess = () => resolve(req.result ?? null);
    req.onerror = () => reject(req.error);
  });
}

function _idbGetAll() {
  return _idbTx("readonly", (store, resolve, reject) => {
    const req = store.getAll();
    req.onsuccess = () => resolve(req.result ?? []);
    req.onerror = () => reject(req.error);
  });
}

function _idbPutMany(monitors) {
  if (!monitors.length) return Promise.resolve();
  return _openIdb().then(db => new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, "readwrite");
    const store = tx.objectStore(IDB_STORE);
    for (const m of monitors) { if (m?.id) store.put(m); }
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error || new Error("IDB transaction aborted"));
  }));
}

function _idbDeleteMany(ids) {
  if (!ids.length) return Promise.resolve();
  return _openIdb().then(db => new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, "readwrite");
    const store = tx.objectStore(IDB_STORE);
    for (const id of ids) store.delete(id);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error || new Error("IDB transaction aborted"));
  }));
}

function _idbCount() {
  return _idbTx("readonly", (store, resolve, reject) => {
    const req = store.count();
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

// One-time migration: copy from chrome.storage → IndexedDB.
// Safe to call on every startup — no-ops if IDB already has data.
export async function migrateToPerMonitorStorage() {
  try {
    const count = await _idbCount();
    if (count > 0) return; // Already populated — nothing to do.

    // Prefer per-monitor keys (most recent chrome.storage format).
    const MIGRATION_FLAG_KEY = "monitors:migrated";
    const MONITORS_INDEX_KEY = "monitors:index";
    const flagStored = await chrome.storage.local.get(MIGRATION_FLAG_KEY);
    if (flagStored[MIGRATION_FLAG_KEY]) {
      const idxStored = await chrome.storage.local.get(MONITORS_INDEX_KEY);
      const ids = Array.isArray(idxStored[MONITORS_INDEX_KEY]) ? idxStored[MONITORS_INDEX_KEY] : [];
      if (ids.length) {
        const all = [];
        const CHUNK = 100;
        for (let i = 0; i < ids.length; i += CHUNK) {
          const slice = ids.slice(i, i + CHUNK);
          const stored = await chrome.storage.local.get(slice.map(id => `monitor:${id}`));
          for (const id of slice) {
            const m = stored[`monitor:${id}`];
            if (m?.id) all.push(m);
          }
        }
        if (all.length) { await _idbPutMany(all); return; }
      }
    }

    // Fall back to the original pageMonitors blob.
    const stored = await chrome.storage.local.get(STORAGE_KEY);
    const monitors = Array.isArray(stored[STORAGE_KEY]) ? stored[STORAGE_KEY] : [];
    if (monitors.length) await _idbPutMany(monitors);
  } catch (err) {
    console.error("IDB migration failed — data still safe in chrome.storage:", err);
  }
}

export async function getMonitorById(id) {
  return _idbGetOne(id);
}

export async function saveMonitorById(monitor) {
  if (!monitor?.id) return;
  await _idbPutMany([compactMonitorForStorage(monitor, 1)]);
}

export async function deleteMonitorsByIds(ids) {
  await _idbDeleteMany(ids);
}

export async function getMonitors() {
  let monitors = await _idbGetAll();
  if (!monitors.length) {
    await migrateToPerMonitorStorage();
    monitors = await _idbGetAll();
  }
  if (!monitors.length) {
    const stored = await chrome.storage.local.get(STORAGE_KEY);
    const legacyMonitors = Array.isArray(stored[STORAGE_KEY]) ? stored[STORAGE_KEY] : [];
    if (legacyMonitors.length) {
      await _idbPutMany(legacyMonitors);
      monitors = await _idbGetAll();
    }
  }
  const normalized = normalizeMonitors(monitors);
  const compacted = normalized.monitors.map((monitor) => compactMonitorForStorage(monitor, 1));
  const compactedChanged = compacted.some((monitor, index) => {
    const original = normalized.monitors[index] || {};
    return (
      monitor.lastHtmlSnapshot !== original.lastHtmlSnapshot ||
      monitor.previousHtmlSnapshot !== original.previousHtmlSnapshot ||
      monitor.lastHtmlDiff !== original.lastHtmlDiff ||
      monitor.lastSelectedOuterHtmlSnapshot !== original.lastSelectedOuterHtmlSnapshot ||
      monitor.previousSelectedOuterHtmlSnapshot !== original.previousSelectedOuterHtmlSnapshot ||
      monitor.initialFullPageText !== original.initialFullPageText ||
      monitor.lastSnapshot !== original.lastSnapshot ||
      monitor.previousSnapshot !== original.previousSnapshot ||
      JSON.stringify(monitor.changeHistory || []) !== JSON.stringify(original.changeHistory || [])
    );
  });
  if (normalized.changed || compactedChanged) await _idbPutMany(compacted);
  return compacted;
}

const STORAGE_BACKUP_KEY = "_monitorsBackup";

// Writes a compact (no HTML) snapshot to chrome.storage.local as automatic backup.
// Survives browser restarts and extension reloads with zero user interaction.
export async function saveStorageBackup(monitors) {
  if (!monitors.length) return;
  const compact = monitors.map((m) => compactMonitorForStorage(m, 1));
  try {
    await chrome.storage.local.set({ [STORAGE_BACKUP_KEY]: compact });
  } catch (_) {}
}

// Restores monitors from chrome.storage.local backup into IDB.
// Called automatically on startup when IDB is empty.
export async function restoreFromStorageBackup() {
  try {
    const stored = await chrome.storage.local.get(STORAGE_BACKUP_KEY);
    const backup = stored[STORAGE_BACKUP_KEY];
    if (!Array.isArray(backup) || !backup.length) return 0;
    await _idbPutMany(backup);
    return backup.length;
  } catch (_) {
    return 0;
  }
}

// Replaces the full monitor set. Handles deletions by comparing current IDB keys.
export async function saveMonitors(monitors) {
  const normalized = normalizeMonitors(monitors);
  const compacted = normalized.monitors.map((monitor) => compactMonitorForStorage(monitor, 1));
  const db = await _openIdb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE, "readwrite");
    const store = tx.objectStore(IDB_STORE);
    const keysReq = store.getAllKeys();
    keysReq.onsuccess = () => {
      const currentIds = new Set(keysReq.result);
      const newIds = new Set(compacted.map(m => m.id).filter(Boolean));
      for (const id of currentIds) { if (!newIds.has(id)) store.delete(id); }
      for (const m of compacted) { if (m?.id) store.put(m); }
    };
    keysReq.onerror = () => reject(keysReq.error);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error || new Error("IDB transaction aborted"));
  });
}

function isQuotaError(error) {
  const message = String(error?.message || error || "").toLowerCase();
  return message.includes("quota") || message.includes("kquotabytes");
}

function trimText(value, maxLength) {
  const text = typeof value === "string" ? value : "";
  if (!maxLength || text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}…`;
}

function compactChangeHistoryEntry(entry, dropText = false) {
  if (!entry || typeof entry !== "object") return entry;
  return {
    ...entry,
    previousText: dropText ? "" : trimText(entry.previousText, 500),
    currentText: dropText ? "" : trimText(entry.currentText, 500),
    previousHtml: "",
    currentHtml: "",
    htmlDiff: ""
  };
}

function compactMonitorForStorage(monitor, level = 1) {
  if (!monitor || typeof monitor !== "object") return monitor;
  if (level <= 0) return monitor;

  const compacted = { ...monitor };

  compacted.lastHtmlSnapshot = "";
  compacted.previousHtmlSnapshot = "";
  compacted.lastHtmlDiff = "";
  compacted.lastSelectedOuterHtmlSnapshot = trimText(compacted.lastSelectedOuterHtmlSnapshot, 300000);
  compacted.previousSelectedOuterHtmlSnapshot = trimText(compacted.previousSelectedOuterHtmlSnapshot, 300000);

  if (level >= 1) {
    compacted.initialFullPageText = trimText(compacted.initialFullPageText, 4000);
    compacted.lastSnapshot = trimText(compacted.lastSnapshot, 4000);
    compacted.previousSnapshot = trimText(compacted.previousSnapshot, 4000);
    compacted.changeHistory = Array.isArray(compacted.changeHistory)
      ? compacted.changeHistory.slice(0, 5).map((entry) => compactChangeHistoryEntry(entry, false))
      : [];
  }

  if (level >= 2) {
    compacted.initialFullPageText = "";
    compacted.lastSnapshot = trimText(compacted.lastSnapshot, 1000);
    compacted.previousSnapshot = trimText(compacted.previousSnapshot, 1000);
    compacted.lastSelectedOuterHtmlSnapshot = trimText(compacted.lastSelectedOuterHtmlSnapshot, 50000);
    compacted.previousSelectedOuterHtmlSnapshot = trimText(compacted.previousSelectedOuterHtmlSnapshot, 50000);
    compacted.changeHistory = Array.isArray(compacted.changeHistory)
      ? compacted.changeHistory.slice(0, 2).map((entry) => compactChangeHistoryEntry(entry, true))
      : [];
  }

  return compacted;
}

async function persistMonitors(monitors) {
  try {
    await chrome.storage.local.set({ [STORAGE_KEY]: monitors });
  } catch (error) {
    if (!isQuotaError(error)) throw error;
    try {
      const compacted = monitors.map((monitor) => compactMonitorForStorage(monitor, 1));
      await chrome.storage.local.set({ [STORAGE_KEY]: compacted });
    } catch (secondError) {
      if (!isQuotaError(secondError)) throw secondError;
      const compacted = monitors.map((monitor) => compactMonitorForStorage(monitor, 2));
      await chrome.storage.local.set({ [STORAGE_KEY]: compacted });
    }
  }
}

export function intervalLabel(minutes) {
  if (minutes >= 1440 && minutes % 1440 === 0) {
    const days = minutes / 1440;
    return days === 1 ? "1 day" : `${days} days`;
  }

  if (minutes >= 60 && minutes % 60 === 0) {
    const hours = minutes / 60;
    return hours === 1 ? "1 hr" : `${hours} hr`;
  }

  return `${minutes} min`;
}

export function buildHtmlDiff(previousHtml, currentHtml) {
  return buildDiffRows(previousHtml, currentHtml)
    .flatMap((row) => {
      const lines = [];

      if (row.leftType === "removed" && row.left) {
        lines.push(`- ${row.left}`);
      }

      if (row.rightType === "added" && row.right) {
        lines.push(`+ ${row.right}`);
      }

      return lines;
    })
    .join("\n");
}

export function buildDiffRows(previousHtml, currentHtml) {
  const beforeLines = splitHtmlLines(previousHtml);
  const afterLines = splitHtmlLines(currentHtml);
  const lcs = buildLcsMatrix(beforeLines, afterLines);
  const operations = [];
  let beforeIndex = beforeLines.length;
  let afterIndex = afterLines.length;

  while (beforeIndex > 0 && afterIndex > 0) {
    if (beforeLines[beforeIndex - 1] === afterLines[afterIndex - 1]) {
      operations.push({
        kind: "same",
        value: beforeLines[beforeIndex - 1]
      });
      beforeIndex -= 1;
      afterIndex -= 1;
    } else if (lcs[beforeIndex - 1][afterIndex] >= lcs[beforeIndex][afterIndex - 1]) {
      operations.push({
        kind: "removed",
        value: beforeLines[beforeIndex - 1]
      });
      beforeIndex -= 1;
    } else {
      operations.push({
        kind: "added",
        value: afterLines[afterIndex - 1]
      });
      afterIndex -= 1;
    }
  }

  while (beforeIndex > 0) {
    operations.push({
      kind: "removed",
      value: beforeLines[beforeIndex - 1]
    });
    beforeIndex -= 1;
  }

  while (afterIndex > 0) {
    operations.push({
      kind: "added",
      value: afterLines[afterIndex - 1]
    });
    afterIndex -= 1;
  }

  operations.reverse();

  const rows = [];
  for (let index = 0; index < operations.length; index += 1) {
    const current = operations[index];

    if (current.kind === "same") {
      rows.push({
        left: current.value,
        right: current.value,
        leftType: "same",
        rightType: "same"
      });
      continue;
    }

    if (current.kind === "removed") {
      const next = operations[index + 1];
      if (next?.kind === "added") {
        rows.push({
          left: current.value,
          right: next.value,
          leftType: "removed",
          rightType: "added"
        });
        index += 1;
        continue;
      }

      rows.push({
        left: current.value,
        right: "",
        leftType: "removed",
        rightType: "empty"
      });
      continue;
    }

    rows.push({
      left: "",
      right: current.value,
      leftType: "empty",
      rightType: "added"
    });
  }

  return rows;
}

export function summarizeDiff(rows) {
  const summary = {
    added: 0,
    removed: 0,
    changed: 0,
    unchanged: 0
  };

  for (const row of rows) {
    if (row.leftType === "same" && row.rightType === "same") {
      summary.unchanged += 1;
      continue;
    }

    if (row.leftType === "removed" && row.rightType === "added") {
      summary.changed += 1;
      continue;
    }

    if (row.leftType === "removed") {
      summary.removed += 1;
    }

    if (row.rightType === "added") {
      summary.added += 1;
    }
  }

  return summary;
}

function splitHtmlLines(html) {
  return (html ?? "")
    .replace(/></g, ">\n<")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function buildLcsMatrix(beforeLines, afterLines) {
  const rows = beforeLines.length + 1;
  const cols = afterLines.length + 1;
  const matrix = Array.from({ length: rows }, () => Array(cols).fill(0));

  for (let i = 1; i < rows; i += 1) {
    for (let j = 1; j < cols; j += 1) {
      if (beforeLines[i - 1] === afterLines[j - 1]) {
        matrix[i][j] = matrix[i - 1][j - 1] + 1;
      } else {
        matrix[i][j] = Math.max(matrix[i - 1][j], matrix[i][j - 1]);
      }
    }
  }

  return matrix;
}

