from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from math import atan2, degrees, hypot
from pathlib import Path
import argparse
import re

DEFAULT_INPUT_DIR = str(Path(__file__).parent / "input")
DEFAULT_OUTPUT_DIR = str(Path(__file__).parent / "output")

NUMBER = r"[-+]?(?:\d*\.\d+|\d+)(?:[eE][-+]?\d+)?"
NUMBER_RE = re.compile(rf"^{NUMBER}$")


@dataclass
class FeatureStore:
    total: int
    removed: int
    angles: Counter
    ops: Counter
    targets: Counter
    scale_x: list[float]
    scale_y: list[float]


def new_store() -> FeatureStore:
    return FeatureStore(
        total=0,
        removed=0,
        angles=Counter(),
        ops=Counter(),
        targets=Counter(),
        scale_x=[],
        scale_y=[],
    )


def merge_store(dst: FeatureStore, src: FeatureStore) -> None:
    dst.total += src.total
    dst.removed += src.removed
    dst.angles.update(src.angles)
    dst.ops.update(src.ops)
    dst.targets.update(src.targets)
    dst.scale_x.extend(src.scale_x)
    dst.scale_y.extend(src.scale_y)


def collect_pdf_files(folder: Path) -> list[Path]:
    return [p for p in folder.rglob("*.pdf") if p.is_file()]


def build_patterns() -> dict[str, re.Pattern]:
    tm_pattern = re.compile(
        rf"({NUMBER})\s+({NUMBER})\s+({NUMBER})\s+({NUMBER})\s+({NUMBER})\s+({NUMBER})\s+Tm"
    )
    cm_pattern = re.compile(
        rf"({NUMBER})\s+({NUMBER})\s+({NUMBER})\s+({NUMBER})\s+({NUMBER})\s+({NUMBER})\s+cm"
    )
    block_pattern = re.compile(r"BT[\s\S]*?ET")
    artifact_pattern = re.compile(
        r"/Artifact\s*<<[\s\S]*?(?:/Subtype\s*/Watermark|/Type\s*/Pagination)[\s\S]*?>>\s*BDC[\s\S]*?EMC"
        r"(?:\s*(?:/[a-zA-Z0-9_]+|[a-zA-Z0-9_]+)\s+Do(?:\s*Q)?)?"
    )
    q_block_pattern = re.compile(r"q[\s\S]*?Q")
    paint_pattern = re.compile(r"(?:^|\s)(?:S|s|f\*?|F|B\*?|b\*?|n)(?:\s|$)")
    cm_do_pattern = re.compile(rf"{NUMBER}\s+{NUMBER}\s+{NUMBER}\s+{NUMBER}\s+{NUMBER}\s+{NUMBER}\s+cm[\s\S]{{0,200}}?\bDo\b")
    return {
        "tm": tm_pattern,
        "cm": cm_pattern,
        "block": block_pattern,
        "artifact": artifact_pattern,
        "qblock": q_block_pattern,
        "paint": paint_pattern,
        "cm_do": cm_do_pattern,
    }


def matrix_features(a: float, b: float, c: float, d: float, threshold: float) -> dict[str, float] | None:
    if abs(b) <= threshold and abs(c) <= threshold:
        return None
    scale_x = hypot(a, b)
    scale_y = hypot(c, d)
    if scale_x == 0:
        return None
    angle = degrees(atan2(b, a))
    if angle < 0:
        angle += 360
    return {"angle": angle, "scale_x": scale_x, "scale_y": scale_y}


def record_feature(store: FeatureStore, feature: dict[str, float], op: str, target: str) -> None:
    store.total += 1
    angle_bin = round(feature["angle"] / 5) * 5
    store.angles[angle_bin] += 1
    store.ops[op] += 1
    store.targets[target] += 1
    store.scale_x.append(feature["scale_x"])
    store.scale_y.append(feature["scale_y"])


def find_features(text: str, pattern: re.Pattern, threshold: float) -> list[dict[str, float]]:
    features = []
    for match in pattern.finditer(text):
        try:
            a, b, c, d, _, _ = map(float, match.groups())
        except ValueError:
            continue
        feature = matrix_features(a, b, c, d, threshold)
        if feature:
            features.append(feature)
    return features


def tokenize_content(text: str) -> list[str]:
    tokens = []
    i = 0
    length = len(text)
    delimiters = set("[]()<>/%")
    while i < length:
        ch = text[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "%":
            while i < length and text[i] not in "\r\n":
                i += 1
            continue
        if ch == "(":
            start = i
            depth = 1
            i += 1
            while i < length and depth > 0:
                if text[i] == "\\":
                    i += 2
                    continue
                if text[i] == "(":
                    depth += 1
                elif text[i] == ")":
                    depth -= 1
                i += 1
            tokens.append(text[start:i])
            continue
        if ch == "<":
            if i + 1 < length and text[i + 1] == "<":
                tokens.append("<<")
                i += 2
                continue
            start = i
            i += 1
            while i < length and text[i] != ">":
                i += 1
            i += 1
            tokens.append(text[start:i])
            continue
        if ch == ">":
            if i + 1 < length and text[i + 1] == ">":
                tokens.append(">>")
                i += 2
            else:
                tokens.append(">")
                i += 1
            continue
        if ch == "/":
            start = i
            i += 1
            while i < length and not text[i].isspace() and text[i] not in delimiters:
                i += 1
            tokens.append(text[start:i])
            continue
        start = i
        i += 1
        while i < length and not text[i].isspace() and text[i] not in delimiters:
            i += 1
        tokens.append(text[start:i])
    return tokens


def is_number_token(token: str) -> bool:
    return NUMBER_RE.match(token) is not None


def segment_feature(p1: tuple[float, float], p2: tuple[float, float]) -> dict[str, float]:
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    angle = degrees(atan2(dy, dx))
    if angle < 0:
        angle += 360
    return {"angle": angle, "scale_x": abs(dx), "scale_y": abs(dy)}


def remove_rotated_qblocks(text: str, store: FeatureStore, threshold: float) -> tuple[str, int]:
    tokens = tokenize_content(text)
    output_tokens: list[str] = []
    operands: list[float] = []
    removed_count = 0
    paint_ops = {"S", "s", "f", "F", "f*", "B", "B*", "b", "b*"}
    stack: list[dict[str, object]] = []
    for token in tokens:
        output_tokens.append(token)
        if is_number_token(token):
            operands.append(float(token))
            continue
        if token == "q":
            stack.append(
                {
                    "start": len(output_tokens) - 1,
                    "rotated": False,
                    "has_paint": False,
                    "has_do": False,
                    "features": [],
                }
            )
            operands.clear()
            continue
        if token == "Q":
            operands.clear()
            if stack:
                state = stack.pop()
                if state["rotated"] and (state["has_paint"] or state["has_do"]):
                    start = state["start"]
                    del output_tokens[start:]
                    target = "xobject" if state["has_do"] else "shape"
                    for feature in state["features"]:
                        record_feature(store, feature, "cm", target)
                    removed_count += 1
            continue
        if token == "Do":
            if stack:
                stack[-1]["has_do"] = True
            operands.clear()
            continue
        if token in paint_ops:
            if stack:
                stack[-1]["has_paint"] = True
            operands.clear()
            continue
        if token == "cm" and len(operands) >= 6:
            a, b, c, d, _, _ = operands[-6:]
            feature = matrix_features(a, b, c, d, threshold)
            if feature and stack:
                stack[-1]["rotated"] = True
                stack[-1]["features"].append(feature)
            operands.clear()
            continue
        operands.clear()
    return " ".join(output_tokens), removed_count


def remove_tilted_paths(text: str, store: FeatureStore, threshold: float) -> tuple[str, int]:
    tokens = tokenize_content(text)
    output_tokens: list[str] = []
    operands: list[float] = []
    in_text = False
    path_active = False
    path_tilted = False
    path_feature: dict[str, float] | None = None
    current_point: tuple[float, float] | None = None
    path_threshold = max(threshold * 10, 0.5)
    removed_count = 0
    paint_ops = {"S", "s", "f", "F", "f*", "B", "B*", "b", "b*"}
    for token in tokens:
        output_tokens.append(token)
        if is_number_token(token):
            operands.append(float(token))
            continue
        if token == "BT":
            in_text = True
            operands.clear()
            continue
        if token == "ET":
            in_text = False
            operands.clear()
            current_point = None
            continue
        if in_text:
            operands.clear()
            continue
        if token in {"m", "l", "c", "v", "y", "h", "re"}:
            path_active = True
            if token == "m" and len(operands) >= 2:
                current_point = (operands[-2], operands[-1])
            elif token == "l" and len(operands) >= 2 and current_point:
                new_point = (operands[-2], operands[-1])
                if abs(new_point[0] - current_point[0]) > path_threshold and abs(new_point[1] - current_point[1]) > path_threshold:
                    path_tilted = True
                    if path_feature is None:
                        path_feature = segment_feature(current_point, new_point)
                current_point = new_point
            elif token == "c" and len(operands) >= 6 and current_point:
                new_point = (operands[-2], operands[-1])
                if abs(new_point[0] - current_point[0]) > path_threshold and abs(new_point[1] - current_point[1]) > path_threshold:
                    path_tilted = True
                    if path_feature is None:
                        path_feature = segment_feature(current_point, new_point)
                current_point = new_point
            elif token in {"v", "y"} and len(operands) >= 4 and current_point:
                new_point = (operands[-2], operands[-1])
                if abs(new_point[0] - current_point[0]) > path_threshold and abs(new_point[1] - current_point[1]) > path_threshold:
                    path_tilted = True
                    if path_feature is None:
                        path_feature = segment_feature(current_point, new_point)
                current_point = new_point
            elif token == "re" and len(operands) >= 4:
                current_point = (operands[-4], operands[-3])
            operands.clear()
            continue
        if token == "n":
            path_active = False
            path_tilted = False
            path_feature = None
            current_point = None
            operands.clear()
            continue
        if token in paint_ops and path_active:
            if path_tilted:
                output_tokens[-1] = "n"
                if path_feature:
                    record_feature(store, path_feature, "path", "shape")
                removed_count += 1
            path_active = False
            path_tilted = False
            path_feature = None
            current_point = None
            operands.clear()
            continue
        operands.clear()
    return " ".join(output_tokens), removed_count


def analyze_and_clean_stream(text: str, patterns: dict[str, re.Pattern], threshold: float) -> tuple[str, int, FeatureStore]:
    store = new_store()
    removed_count = 0

    def replace_block(match: re.Match) -> str:
        nonlocal removed_count
        block = match.group(0)
        features = find_features(block, patterns["tm"], threshold)
        if features:
            for feature in features:
                record_feature(store, feature, "Tm", "text")
            removed_count += 1
            return ""
        return block

    new_text = patterns["block"].sub(replace_block, text)

    def replace_artifact(match: re.Match) -> str:
        nonlocal removed_count
        block = match.group(0)
        features = find_features(block, patterns["cm"], threshold)
        if features:
            for feature in features:
                record_feature(store, feature, "cm", "artifact")
            removed_count += 1
            return ""
        if re.search(r"/Subtype\s*/Watermark", block, re.IGNORECASE):
            removed_count += 1
            return ""
        return block

    new_text = patterns["artifact"].sub(replace_artifact, new_text)

    def replace_cm_do(match: re.Match) -> str:
        nonlocal removed_count
        block = match.group(0)
        features = find_features(block, patterns["cm"], threshold)
        if features:
            for feature in features:
                record_feature(store, feature, "cm", "xobject")
            removed_count += 1
            return ""
        return block

    new_text = patterns["cm_do"].sub(replace_cm_do, new_text)

    new_text, removed_qblocks = remove_rotated_qblocks(new_text, store, threshold)
    removed_count += removed_qblocks

    new_text, removed_paths = remove_tilted_paths(new_text, store, threshold)
    removed_count += removed_paths

    store.removed += removed_count
    return new_text, removed_count, store


def format_store(store: FeatureStore) -> str:
    angle_items = ", ".join(f"{k}°:{v}" for k, v in store.angles.most_common(8))
    avg_scale_x = sum(store.scale_x) / len(store.scale_x) if store.scale_x else 0
    avg_scale_y = sum(store.scale_y) / len(store.scale_y) if store.scale_y else 0
    return (
        f"倾斜元素={store.total}, 移除={store.removed}, "
        f"角度分布[{angle_items}], "
        f"平均缩放=({avg_scale_x:.2f}, {avg_scale_y:.2f}), "
        f"类型={dict(store.targets)}"
    )


def parse_page_indices(value: str | None) -> set[int] | None:
    if not value:
        return None
    indices: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            start_i = int(start) - 1
            end_i = int(end) - 1
            for i in range(start_i, end_i + 1):
                indices.add(i)
        else:
            indices.add(int(part) - 1)
    return indices


def remove_tilted_elements(
    pdf_path: Path,
    output_dir: Path,
    page_indices: set[int] | None = None,
    threshold: float = 0.05,
) -> Path | None:
    try:
        import fitz
    except ImportError:
        print("未安装 PyMuPDF(fitz)，无法处理 PDF")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / pdf_path.name

    try:
        doc = fitz.open(pdf_path)
    except Exception as exc:
        print(f"打开失败: {pdf_path} ({exc})")
        return None

    page_rotations = [doc[i].rotation for i in range(len(doc))]

    patterns = build_patterns()
    print(f"正在处理: {pdf_path.name}")

    total_store = new_store()
    page_indices_set = set(page_indices) if page_indices else None

    for page_index in range(len(doc)):
        if page_indices_set is not None and page_index not in page_indices_set:
            continue
        page = doc[page_index]
        page_store = new_store()

        xrefs = page.get_contents() or []
        for xref in xrefs:
            try:
                stream = doc.xref_stream(xref)
            except Exception:
                continue
            text = stream.decode("latin1", errors="ignore")
            new_text, removed, store = analyze_and_clean_stream(text, patterns, threshold)
            if removed > 0:
                doc.update_stream(xref, new_text.encode("latin1"))
            merge_store(page_store, store)

        for item in page.get_xobjects():
            xref = item[0]
            try:
                obj_str = doc.xref_object(xref)
                if "/PieceInfo" in obj_str and "/Watermark" in obj_str:
                    doc.update_stream(xref, b"")
                    page_store.removed += 1
                    continue
            except Exception:
                pass

            try:
                stream = doc.xref_stream(xref)
            except Exception:
                continue
            text = stream.decode("latin1", errors="ignore")
            new_text, removed, store = analyze_and_clean_stream(text, patterns, threshold)
            if removed > 0:
                doc.update_stream(xref, new_text.encode("latin1"))
            merge_store(page_store, store)

        merge_store(total_store, page_store)

    for page_index, rotation in enumerate(page_rotations):
        doc[page_index].set_rotation(rotation)

    if total_store.total == 0:
        print("  - 未发现倾斜水印或倾斜形状")
    else:
        print(f"  - 汇总: {format_store(total_store)}")

    try:
        doc.save(output_file, deflate=True)
    except Exception as exc:
        print(f"保存失败: {output_file} ({exc})")
        return None
    finally:
        doc.close()

    return output_file


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--pages", default="")
    parser.add_argument("--threshold", type=float, default=0.05)
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        print(f"输入路径不存在: {input_dir}")
        return

    pdfs = collect_pdf_files(input_dir)
    print(f"扫描到 {len(pdfs)} 个PDF文件")

    page_indices = parse_page_indices(args.pages)
    processed_count = 0
    for pdf in pdfs:
        out = remove_tilted_elements(pdf, output_dir, page_indices, args.threshold)
        if out:
            processed_count += 1
            print(f"  - 输出文件: {out}")

    print(f"\n处理完成: 共处理 {processed_count} 个文件")


if __name__ == "__main__":
    main()
