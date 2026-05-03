import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set
import xml.etree.ElementTree as ET
from collections import deque, defaultdict
import heapq
import textwrap
from pathlib import Path


#IR (Intermediate Representation)
@dataclass
class IRStep:
    step_id: str
    step_type: str
    name: Optional[str] = None
    description: Optional[str] = None

    #transitions can be str or list[str]
    on_completion: Optional[object] = None
    on_success: Optional[object] = None
    on_failure: Optional[object] = None
    on_true: Optional[object] = None
    on_false: Optional[object] = None
    next_steps: Optional[List[str]] = None

    cases: Optional[dict] = None
    default: Optional[object] = None
    condition: Optional[str] = None

    #Optional CACAO step details
    delay: Optional[object] = None
    timeout: Optional[object] = None
    step_variables: Optional[dict] = None
    in_args: Optional[List[str]] = None
    out_args: Optional[List[str]] = None
    markings: Optional[List[str]] = None
    step_extensions: Optional[dict] = None

    #Playbook-Action details
    commands: Optional[list] = None
    agent: Optional[str] = None
    targets: Optional[list] = None
    playbook_id: Optional[str] = None
    playbook_version: Optional[str] = None


@dataclass
class IRPlaybook:
    playbook_id: str
    name: Optional[str]
    description: Optional[str]
    workflow_start: str
    steps: Dict[str, IRStep]
    agent_definitions: Dict[str, dict]
    target_definitions: Dict[str, dict]
    authentication_info_definitions: Dict[str, dict]
    extension_definitions: Dict[str, dict]
    data_marking_definitions: Dict[str, dict]
    created: Optional[str] = None
    modified: Optional[str] = None
    spec_version: Optional[str] = None
    playbook_types: Optional[List[str]] = None
    playbook_activities: Optional[List[str]] = None
    playbook_processing_summary: Optional[List[str]] = None
    playbook_variables: Optional[dict] = None
    created_by: Optional[str] = None
    valid_from: Optional[str] = None
    valid_until: Optional[str] = None
    revoked: Optional[bool] = None
    derived_from: Optional[List[str]] = None
    labels: Optional[List[str]] = None
    markings: Optional[List[str]] = None
    external_references: Optional[List[dict]] = None
    industry_sectors: Optional[List[str]] = None
    signatures: Optional[List[dict]] = None
    workflow_exception: Optional[str] = None


def load_cacao_playbook(path: str) -> IRPlaybook:
    with open(path, "r", encoding="utf-8") as f:
        pb = json.load(f)

    if pb.get("type") != "playbook":
        raise ValueError("Input JSON is not a CACAO playbook (type != 'playbook').")

    workflow = pb.get("workflow", {})
    workflow_start = pb.get("workflow_start")
    if not workflow_start or workflow_start not in workflow:
        raise ValueError("Missing or invalid workflow_start.")

    steps: Dict[str, IRStep] = {}
    for step_id, step_obj in workflow.items():
        stype = step_obj.get("type")
        if not stype:
            raise ValueError(f"Workflow step {step_id} has no type.")

        steps[step_id] = IRStep(
            step_id=step_id,
            step_type=stype,
            name=step_obj.get("name"),
            description=step_obj.get("description"),

            on_completion=step_obj.get("on_completion"),
            on_success=step_obj.get("on_success"),
            on_failure=step_obj.get("on_failure"),
            on_true=step_obj.get("on_true"),
            on_false=step_obj.get("on_false"),
            next_steps=step_obj.get("next_steps"),

            cases=step_obj.get("cases"),
            default=step_obj.get("default"),
            condition=step_obj.get("condition"),
            delay=step_obj.get("delay"),
            timeout=step_obj.get("timeout"),
            step_variables=step_obj.get("step_variables"),
            in_args=step_obj.get("in_args"),
            out_args=step_obj.get("out_args"),
            markings=step_obj.get("markings"),
            step_extensions=step_obj.get("step_extensions"),

            commands=step_obj.get("commands"),
            agent=step_obj.get("agent"),
            targets=step_obj.get("targets"),
            playbook_id=step_obj.get("playbook_id"),
            playbook_version=step_obj.get("playbook_version"),
        )

    return IRPlaybook(
        playbook_id=pb.get("id", str(uuid.uuid4())),
        name=pb.get("name"),
        description=pb.get("description"),
        workflow_start=workflow_start,
        steps=steps,
        agent_definitions=pb.get("agent_definitions", {}),
        target_definitions=pb.get("target_definitions", {}),
        authentication_info_definitions=pb.get("authentication_info_definitions", {}),
        extension_definitions=pb.get("extension_definitions", {}),
        data_marking_definitions=pb.get("data_marking_definitions", {}),
        created=pb.get("created"),
        modified=pb.get("modified"),
        spec_version=pb.get("spec_version"),
        playbook_types=pb.get("playbook_types"),
        playbook_activities=pb.get("playbook_activities"),
        playbook_processing_summary=pb.get("playbook_processing_summary"),
        playbook_variables=pb.get("playbook_variables"),
        created_by=pb.get("created_by"),
        valid_from=pb.get("valid_from"),
        valid_until=pb.get("valid_until"),
        revoked=pb.get("revoked"),
        derived_from=pb.get("derived_from"),
        labels=pb.get("labels"),
        markings=pb.get("markings"),
        external_references=pb.get("external_references"),
        industry_sectors=pb.get("industry_sectors"),
        signatures=pb.get("signatures"),
        workflow_exception=pb.get("workflow_exception"),
    )


#BPMN dependencies + Diagram Interchange Namespaces
BPMN_NS = "http://www.omg.org/spec/BPMN/20100524/MODEL"
BPMNDI_NS = "http://www.omg.org/spec/BPMN/20100524/DI"
DC_NS = "http://www.omg.org/spec/DD/20100524/DC"
DI_NS = "http://www.omg.org/spec/DD/20100524/DI"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
CACAO_EXT_NS = "https://example.org/cacao-bpmn/extensions"

ET.register_namespace("bpmn", BPMN_NS)
ET.register_namespace("bpmndi", BPMNDI_NS)
ET.register_namespace("dc", DC_NS)
ET.register_namespace("di", DI_NS)
ET.register_namespace("xsi", XSI_NS)
ET.register_namespace("cacao", CACAO_EXT_NS)


def _ns(ns: str, tag: str) -> str:
    return f"{{{ns}}}{tag}"


def _bpmn(tag: str) -> str:
    return _ns(BPMN_NS, tag)


def _bpmndi(tag: str) -> str:
    return _ns(BPMNDI_NS, tag)


def _dc(tag: str) -> str:
    return _ns(DC_NS, tag)


def _di(tag: str) -> str:
    return _ns(DI_NS, tag)


def _cacao(tag: str) -> str:
    return _ns(CACAO_EXT_NS, tag)


#Helper functions
def _render_compact(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(value)


def _comma_join(values: List[str], limit: int = 6) -> str:
    vals = [str(v) for v in values if v not in (None, "")]
    if not vals:
        return ""
    if len(vals) <= limit:
        return ", ".join(vals)
    shown = ", ".join(vals[:limit])
    return f"{shown}, ... (+{len(vals) - limit} more)"


def _as_list(value) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _summarize_variable_dict(var_dict: Optional[dict], limit: int = 6) -> str:
    if not isinstance(var_dict, dict) or not var_dict:
        return ""
    parts: List[str] = []
    for idx, (var_name, var_obj) in enumerate(var_dict.items()):
        if idx >= limit:
            parts.append(f"... (+{len(var_dict) - limit} more)")
            break
        if isinstance(var_obj, dict):
            bits = []
            vtype = var_obj.get("type")
            constant = var_obj.get("constant")
            data_type = var_obj.get("data_type")
            value = var_obj.get("value")
            if vtype:
                bits.append(f"type={vtype}")
            if data_type:
                bits.append(f"data_type={data_type}")
            if constant is not None:
                bits.append(f"constant={constant}")
            if value is not None and value != "":
                bits.append(f"value={_short(_render_compact(value), 60)}")
            parts.append(f"{var_name} ({', '.join(bits)})" if bits else str(var_name))
        else:
            parts.append(str(var_name))
    return ", ".join(parts)


def _summarize_dict_keys(obj: Optional[dict], label_key_candidates: Optional[List[str]] = None, limit: int = 5) -> str:
    if not isinstance(obj, dict) or not obj:
        return ""
    label_key_candidates = label_key_candidates or ["name", "type", "schema", "tlp", "algorithm", "hash_algorithm"]
    parts: List[str] = []
    for idx, (key, value) in enumerate(obj.items()):
        if idx >= limit:
            parts.append(f"... (+{len(obj) - limit} more)")
            break
        label = None
        if isinstance(value, dict):
            for cand in label_key_candidates:
                if value.get(cand):
                    label = f"{key}:{value.get(cand)}"
                    break
        parts.append(label or str(key))
    return ", ".join(parts)


def _extract_auth_refs(value: Any) -> List[str]:
    refs: List[str] = []

    def visit(obj: Any):
        if isinstance(obj, dict):
            for k, v in obj.items():
                k_low = str(k).lower()
                if "auth" in k_low or "authentication" in k_low:
                    if isinstance(v, str):
                        refs.append(v)
                    elif isinstance(v, list):
                        for item in v:
                            if isinstance(item, str):
                                refs.append(item)
                    elif isinstance(v, dict):
                        if isinstance(v.get("id"), str):
                            refs.append(v["id"])
                visit(v)
        elif isinstance(obj, list):
            for item in obj:
                visit(item)

    visit(value)
    out: List[str] = []
    seen: Set[str] = set()
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            out.append(ref)
    return out


def _summarize_auth_defs(auth_defs: Optional[dict], refs: Optional[List[str]] = None, limit: int = 5) -> str:
    if not isinstance(auth_defs, dict) or not auth_defs:
        return ""
    keys = refs if refs else list(auth_defs.keys())
    parts: List[str] = []
    shown = 0
    for key in keys:
        if shown >= limit:
            parts.append(f"... (+{len(keys) - limit} more)")
            break
        value = auth_defs.get(key)
        if not isinstance(value, dict):
            parts.append(str(key))
        else:
            label = value.get("name") or key
            auth_type = value.get("type")
            detail_keys = ["username", "token_type", "grant_type", "scopes", "user_id"]
            details = []
            for dk in detail_keys:
                if value.get(dk):
                    details.append(f"{dk}={_short(_render_compact(value.get(dk)), 40)}")
            base = f"{label} [{auth_type}]" if auth_type else str(label)
            if details:
                base += f" ({'; '.join(details)})"
            parts.append(base)
        shown += 1
    return ", ".join(parts)


def _entity_summary(entity_id: str, entity_obj: Optional[dict]) -> str:
    if not isinstance(entity_obj, dict):
        return str(entity_id)
    bits: List[str] = []
    name = entity_obj.get("name")
    etype = entity_obj.get("type")
    if name:
        bits.append(str(name))
    elif entity_id:
        bits.append(str(entity_id))
    if etype:
        bits.append(f"type={etype}")
    for key in ("sector", "http_url", "hostname", "fqdn", "path", "user_id", "organization_name", "location", "address", "contact"):
        if entity_obj.get(key):
            bits.append(f"{key}={_short(_render_compact(entity_obj.get(key)), 50)}")
    if entity_obj.get("category"):
        category = entity_obj.get("category")
        if isinstance(category, list):
            bits.append("category=" + _comma_join([str(v) for v in category], limit=4))
        else:
            bits.append(f"category={category}")
    if entity_obj.get("security_category"):
        bits.append(f"security_category={_short(_render_compact(entity_obj.get('security_category')), 50)}")
    return "; ".join(bits)


def _summarize_data_markings(markings: Optional[dict], limit: int = 5) -> str:
    if not isinstance(markings, dict) or not markings:
        return ""
    parts: List[str] = []
    for idx, (key, value) in enumerate(markings.items()):
        if idx >= limit:
            parts.append(f"... (+{len(markings) - limit} more)")
            break
        if isinstance(value, dict):
            label = value.get("name") or key
            if value.get("tlp"):
                parts.append(f"{label} [TLP={value.get('tlp')}]")
            elif value.get("statement"):
                parts.append(f"{label} [{_short(str(value.get('statement')), 50)}]")
            elif value.get("type"):
                parts.append(f"{label} [{value.get('type')}]")
            else:
                parts.append(str(label))
        else:
            parts.append(str(key))
    return ", ".join(parts)


def _summarize_extension_defs(ext_defs: Optional[dict], limit: int = 5) -> str:
    if not isinstance(ext_defs, dict) or not ext_defs:
        return ""
    parts: List[str] = []
    for idx, (key, value) in enumerate(ext_defs.items()):
        if idx >= limit:
            parts.append(f"... (+{len(ext_defs) - limit} more)")
            break
        if isinstance(value, dict):
            label = value.get("name") or key
            details = []
            for dk in ("type", "version", "schema"):
                if value.get(dk):
                    details.append(f"{dk}={_short(str(value.get(dk)), 40)}")
            parts.append(f"{label} ({'; '.join(details)})" if details else str(label))
        else:
            parts.append(str(key))
    return ", ".join(parts)


def _summarize_signatures(signatures: Optional[List[dict]], limit: int = 4) -> str:
    if not isinstance(signatures, list) or not signatures:
        return ""
    parts: List[str] = []
    for idx, sig in enumerate(signatures):
        if idx >= limit:
            parts.append(f"... (+{len(signatures) - limit} more)")
            break
        if isinstance(sig, dict):
            label = sig.get("signature_type") or sig.get("algorithm") or sig.get("hash_algorithm") or "signature"
            details = []
            for dk in ("algorithm", "hash_algorithm", "issuer"):
                if sig.get(dk):
                    details.append(f"{dk}={sig.get(dk)}")
            parts.append(f"{label} ({'; '.join(details)})" if details else str(label))
        else:
            parts.append("signature")
    return ", ".join(parts)


def _summarize_command_list(commands: Optional[list], limit: int = 4) -> str:
    if not isinstance(commands, list) or not commands:
        return ""
    parts: List[str] = []
    for idx, c in enumerate(commands):
        if idx >= limit:
            parts.append(f"... (+{len(commands) - limit} more)")
            break
        if not isinstance(c, dict):
            parts.append(_short(_render_compact(c), 60))
            continue
        ctype = c.get("type") or c.get("command_type") or "command"
        cmd = c.get("command")
        if cmd is not None:
            parts.append(f"{ctype}:{_short(_render_compact(cmd), 50)}")
        else:
            parts.append(str(ctype))
    return ", ".join(parts)


def _wrap_annotation_text(text: str, width_px: int) -> str:
    if not text:
        return ""

    max_chars = max(28, int((width_px - 28) / 6.8))
    wrapped_lines: List[str] = []

    for raw_line in text.splitlines() or [""]:
        line = raw_line.rstrip()
        if not line:
            wrapped_lines.append("")
            continue

        if ":" in line[:20]:
            head, tail = line.split(":", 1)
            initial = f"{head}: "
            rest = tail.strip()
            available = max(12, max_chars - len(initial))
            parts = textwrap.wrap(
                rest,
                width=available,
                break_long_words=False,
                break_on_hyphens=False,
            ) or [""]
            wrapped_lines.append(initial + parts[0])
            subsequent_indent = " " * len(initial)
            for part in parts[1:]:
                wrapped_lines.append(subsequent_indent + part)
        else:
            parts = textwrap.wrap(
                line,
                width=max_chars,
                break_long_words=False,
                break_on_hyphens=False,
            ) or [""]
            wrapped_lines.extend(parts)

    return "\n".join(wrapped_lines)


def _prepare_annotation_box(text: str, min_width: int = 380, max_width: int = 560) -> Tuple[str, int, int]:
    if not text:
        return "", min_width, 70

    step = 40 if max_width - min_width >= 120 else 30
    candidate_widths = list(range(min_width, max_width + 1, step))
    if candidate_widths[-1] != max_width:
        candidate_widths.append(max_width)

    best_wrapped = text
    best_width = min_width
    best_height = 70
    best_score = None

    for width in candidate_widths:
        wrapped = _wrap_annotation_text(text, width)
        line_count = max(1, len(wrapped.splitlines()))
        height = max(76, 30 + line_count * 18)
        score = height * 14 + width
        if best_score is None or score < best_score:
            best_score = score
            best_wrapped = wrapped
            best_width = width
            best_height = height

    return best_wrapped, best_width, best_height


def _add_extension_properties(parent_el: ET.Element, props: Dict[str, Any]) -> None:
    clean_props = {k: v for k, v in props.items() if v not in (None, "", [], {}, ())}
    if not clean_props:
        return
    ext_el = parent_el.find(_bpmn("extensionElements"))
    if ext_el is None:
        ext_el = ET.SubElement(parent_el, _bpmn("extensionElements"))
    for key, value in clean_props.items():
        ET.SubElement(ext_el, _cacao("property"), {
            "name": str(key),
            "value": _render_compact(value),
        })


def iter_transitions(step: IRStep) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []

    def add(label: str, target):
        if not target:
            return
        if isinstance(target, str):
            out.append((label, target))
        elif isinstance(target, list):
            for t in target:
                if isinstance(t, str):
                    out.append((label, t))

    add("on_completion", step.on_completion)
    add("on_success", step.on_success)
    add("on_failure", step.on_failure)
    add("true", step.on_true)
    add("false", step.on_false)
    add("next_step", step.next_steps)

    if isinstance(step.cases, dict):
        for case_key, target in step.cases.items():
            if isinstance(target, str):
                out.append((f"case:{case_key}", target))
            elif isinstance(target, list):
                for t in target:
                    if isinstance(t, str):
                        out.append((f"case:{case_key}", t))

    add("default", step.default)
    return out




def _flow_label_text(label: str) -> Optional[str]:
    if not label:
        return None
    if label == "true":
        return "True"
    if label == "false":
        return "False"
    if label == "on_success":
        return "Success"
    if label == "on_failure":
        return "Failure"
    if label == "default":
        return "Default"
    if label.startswith("case:"):
        return label.split(":", 1)[1]
    return None


def _label_stagger(idx: int) -> int:
    offsets = [0, -18, 18, -36, 36, -54, 54]
    if idx < len(offsets):
        return offsets[idx]
    extra = idx - len(offsets) + 1
    return extra * 18 if extra % 2 else -(extra * 18)


def _shared_prefix_len(a: List[Tuple[int, int]], b: List[Tuple[int, int]]) -> int:
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return i


def _flow_branch_start_index(flow_id: str, points: List[Tuple[int, int]], flow_points_by_id: Dict[str, List[Tuple[int, int]]], sibling_ids: List[str]) -> int:
    if not sibling_ids:
        return 0
    max_shared_prefix = 1
    for sid in sibling_ids:
        other = flow_points_by_id.get(sid)
        if not other:
            continue
        max_shared_prefix = max(max_shared_prefix, _shared_prefix_len(points, other))
    #first segment after the last shared point
    return max(0, min(len(points) - 2, max_shared_prefix - 1))


def _best_flow_label_bounds(
    points: List[Tuple[int, int]],
    text: str,
    rank_offset: int,
    src_bounds: Optional[Tuple[int, int, int, int]] = None,
    tgt_bounds: Optional[Tuple[int, int, int, int]] = None,
    src_kind: Optional[str] = None,
    src_side: Optional[str] = None,
    branch_start_index: int = 0,
) -> Tuple[int, int, int, int]:
    width = max(34, min(96, 14 + len(text) * 8))
    height = 22
    gap = 16

    #Place labels on branch-specific segments to avoid overlap with sequence flows
    segment_candidates = []
    for idx in range(len(points) - 2, branch_start_index - 1, -1):
        x1, y1 = points[idx]
        x2, y2 = points[idx + 1]
        seg_len = abs(x2 - x1) + abs(y2 - y1)
        if seg_len < 22:
            continue
        segment_candidates.append((idx, x1, y1, x2, y2, seg_len))

    #Fallback for short branch-specific segments.
    if not segment_candidates:
        for idx in range(branch_start_index, len(points) - 1):
            x1, y1 = points[idx]
            x2, y2 = points[idx + 1]
            seg_len = abs(x2 - x1) + abs(y2 - y1)
            if seg_len <= 0:
                continue
            segment_candidates.append((idx, x1, y1, x2, y2, seg_len))

    if not segment_candidates:
        return (0, 0, width, height)

    def _horizontal_label(x1: int, y: int, x2: int) -> Tuple[int, int, int, int]:
        t = 0.82 if x2 >= x1 else 0.18
        anchor_x = int(x1 + (x2 - x1) * t)
        lx = anchor_x - width // 2

        #Puts the label clearly above or below the line
        side = -1 if rank_offset <= 0 else 1
        if side < 0:
            ly = y - height - gap + min(rank_offset, 0)
        else:
            ly = y + gap + max(rank_offset, 0)
        return (lx, ly, width, height)

    def _vertical_label(x: int, y1: int, y2: int) -> Tuple[int, int, int, int]:
        t = 0.82 if y2 >= y1 else 0.18
        anchor_y = int(y1 + (y2 - y1) * t)
        ly = anchor_y - height // 2

        #Puts the label clearly left or right of the line
        side = -1 if rank_offset <= 0 else 1
        if side < 0:
            lx = x - width - gap + min(rank_offset, 0)
        else:
            lx = x + gap + max(rank_offset, 0)
        return (lx, ly, width, height)

    for prefer_horizontal in (True, False):
        for idx, x1, y1, x2, y2, seg_len in segment_candidates:
            is_horizontal = (y1 == y2)
            is_vertical = (x1 == x2)
            if prefer_horizontal and not is_horizontal:
                continue
            if (not prefer_horizontal) and not is_vertical:
                continue

            if is_horizontal:
                return _horizontal_label(x1, y1, x2)
            if is_vertical:
                return _vertical_label(x1, y1, y2)

    x1, y1 = points[-2]
    x2, y2 = points[-1]
    if y1 == y2:
        return _horizontal_label(x1, y1, x2)
    if x1 == x2:
        return _vertical_label(x1, y1, y2)
    return (int((x1 + x2) // 2 - width // 2), int((y1 + y2) // 2 - height // 2), width, height)

def _short(s: str, n: int = 140) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 3] + "..."


def _format_annotation_block(prefix: str, parts: List[str], max_len: int = 220) -> str:
    if not parts:
        return ""
    lines: List[str] = []
    current = prefix + ": "
    indent = " " * (len(prefix) + 2)
    for part in parts:
        part = str(part).strip()
        if not part:
            continue
        addition = part if current.endswith(": ") else "; " + part
        if len(current) + len(addition) <= max_len:
            current += addition
        else:
            lines.append(current)
            current = indent + part
    lines.append(current)
    return "\n".join(lines)


def build_meta_text(ir: IRPlaybook) -> str:
    meta_parts: List[str] = [f"id={ir.playbook_id}"]
    if ir.name:
        meta_parts.append(f"name={ir.name}")
    if ir.description:
        meta_parts.append("description=" + _short(ir.description, 220))
    if ir.spec_version:
        meta_parts.append(f"spec_version={ir.spec_version}")
    if ir.playbook_types:
        meta_parts.append("playbook_types=" + _comma_join(ir.playbook_types, limit=6))
    if ir.playbook_activities:
        meta_parts.append("playbook_activities=" + _comma_join(ir.playbook_activities, limit=6))
    if ir.playbook_processing_summary:
        meta_parts.append("processing=" + _comma_join(ir.playbook_processing_summary, limit=6))
    if ir.created:
        meta_parts.append(f"created={ir.created}")
    if ir.modified:
        meta_parts.append(f"modified={ir.modified}")
    if ir.valid_from:
        meta_parts.append(f"valid_from={ir.valid_from}")
    if ir.valid_until:
        meta_parts.append(f"valid_until={ir.valid_until}")
    meta_parts.append(f"workflow_start={ir.workflow_start}")
    if ir.created_by:
        meta_parts.append(f"created_by={ir.created_by}")
    if ir.revoked is not None:
        meta_parts.append(f"revoked={ir.revoked}")
    if ir.derived_from:
        meta_parts.append("derived_from=" + _comma_join(ir.derived_from, limit=4))
    if ir.labels:
        meta_parts.append("labels=" + _comma_join(ir.labels, limit=6))
    if ir.industry_sectors:
        meta_parts.append("industry_sectors=" + _comma_join(ir.industry_sectors, limit=6))
    if ir.playbook_variables:
        meta_parts.append("variables=" + _short(_summarize_variable_dict(ir.playbook_variables, limit=6), 240))
    if ir.authentication_info_definitions:
        meta_parts.append("authentication=" + _short(_summarize_auth_defs(ir.authentication_info_definitions), 240))
    if ir.markings:
        meta_parts.append("marking_refs=" + _comma_join(ir.markings, limit=5))
    if ir.data_marking_definitions:
        meta_parts.append("data_markings=" + _short(_summarize_data_markings(ir.data_marking_definitions), 240))
    if ir.extension_definitions:
        meta_parts.append("extensions=" + _short(_summarize_extension_defs(ir.extension_definitions), 240))
    if ir.signatures:
        meta_parts.append("signatures=" + _short(_summarize_signatures(ir.signatures), 240))
    if ir.external_references:
        meta_parts.append(f"external_references={len(ir.external_references)}")

    return _format_annotation_block("META", meta_parts, max_len=220)
TASK_EXCLUDED_STEP_TYPES = {"start", "end", "if-condition", "switch-condition", "while-condition", "parallel"}


def is_task_like_step(step: IRStep) -> bool:
    return step.step_type not in TASK_EXCLUDED_STEP_TYPES


def _has_nonempty_value(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def step_has_annotation_metadata(step: IRStep) -> bool:
    return any(
        _has_nonempty_value(value)
        for value in (
            step.commands,
            step.agent,
            step.targets,
            step.delay,
            step.timeout,
            step.step_variables,
            step.in_args,
            step.out_args,
            step.markings,
            step.step_extensions,
            step.playbook_id,
            step.playbook_version,
        )
    )


def resolve_agent(ir: IRPlaybook, agent_id: Optional[str]) -> str:
    if not agent_id:
        return "n/a"
    return _entity_summary(agent_id, ir.agent_definitions.get(agent_id, {}))


def resolve_targets(ir: IRPlaybook, target_ids: Optional[list]) -> str:
    if not target_ids:
        return "n/a"
    resolved = []
    for tid in target_ids:
        resolved.append(_entity_summary(str(tid), ir.target_definitions.get(tid, {})))
    return ", ".join(resolved)


def _collect_step_auth_refs(ir: IRPlaybook, step: IRStep) -> List[str]:
    refs: List[str] = []
    if _has_nonempty_value(step.agent):
        refs.extend(_extract_auth_refs(ir.agent_definitions.get(step.agent, {})))
    if _has_nonempty_value(step.targets):
        for tid in _as_list(step.targets):
            refs.extend(_extract_auth_refs(ir.target_definitions.get(tid, {})))
    refs.extend(_extract_auth_refs(step.commands))
    refs.extend(_extract_auth_refs(step.step_extensions))
    out: List[str] = []
    seen: Set[str] = set()
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            out.append(ref)
    return out


def build_task_annotation_text(ir: IRPlaybook, step: IRStep) -> Optional[str]:
    lines: List[str] = []

    meta_parts: List[str] = []
    if _has_nonempty_value(step.delay):
        meta_parts.append(f"delay={_render_compact(step.delay)}")
    if _has_nonempty_value(step.timeout):
        meta_parts.append(f"timeout={_render_compact(step.timeout)}")
    if _has_nonempty_value(step.markings):
        meta_parts.append("markings=" + _comma_join([str(v) for v in _as_list(step.markings)], limit=4))
    if meta_parts:
        lines.append("META: " + "; ".join(meta_parts))

    data_parts: List[str] = []
    if isinstance(step.commands, list):
        cmd_summary = _summarize_command_list(step.commands, limit=4)
        if cmd_summary:
            data_parts.append("commands=" + _short(cmd_summary, 240))
        for i, c in enumerate(step.commands[:4]):
            if not isinstance(c, dict):
                continue
            auth_refs = _extract_auth_refs(c)
            if auth_refs:
                data_parts.append(f"command_auth[{i}]=" + _comma_join(auth_refs, limit=4))
    if _has_nonempty_value(step.in_args):
        data_parts.append("in_args=" + _comma_join([str(v) for v in _as_list(step.in_args)], limit=6))
    if _has_nonempty_value(step.out_args):
        data_parts.append("out_args=" + _comma_join([str(v) for v in _as_list(step.out_args)], limit=6))
    if step.step_type == "playbook-action" and _has_nonempty_value(step.playbook_id):
        data_parts.append(f"playbook_id={step.playbook_id}")
        if _has_nonempty_value(step.playbook_version):
            data_parts.append(f"playbook_version={step.playbook_version}")
    if _has_nonempty_value(step.step_variables):
        data_parts.append("variables=" + _short(_summarize_variable_dict(step.step_variables, limit=6), 220))
    if _has_nonempty_value(step.step_extensions):
        data_parts.append("extensions=" + _short(_summarize_dict_keys(step.step_extensions, ["name", "type", "schema"]), 220))
    auth_refs = _collect_step_auth_refs(ir, step)
    if auth_refs and ir.authentication_info_definitions:
        data_parts.append("authentication=" + _short(_summarize_auth_defs(ir.authentication_info_definitions, refs=auth_refs), 220))
    if data_parts:
        lines.append("DATA: " + ", ".join(data_parts))

    role_parts: List[str] = []
    if _has_nonempty_value(step.agent):
        role_parts.append(f"agent={resolve_agent(ir, step.agent)}")
    if _has_nonempty_value(step.targets):
        role_parts.append(f"targets={resolve_targets(ir, step.targets)}")
    if role_parts:
        lines.append("ROLE: " + "; ".join(role_parts))

    return "\n".join(lines) if lines else None


def _step_display_ref(ir: IRPlaybook, step_id: str) -> str:
    step = ir.steps.get(step_id)
    if not step:
        return step_id
    if step.name:
        return step.name
    return step_id


def _one_armed_if_outcome_note(ir: IRPlaybook, step: IRStep) -> Optional[str]:
    if step.step_type != "if-condition":
        return None
    has_true = _has_nonempty_value(step.on_true)
    has_false = _has_nonempty_value(step.on_false)
    if has_true and not has_false and isinstance(step.on_completion, str) and step.on_completion in ir.steps:
        return f"otherwise/on_completion -> {_step_display_ref(ir, step.on_completion)}"
    if has_false and not has_true and isinstance(step.on_completion, str) and step.on_completion in ir.steps:
        return f"otherwise/on_completion -> {_step_display_ref(ir, step.on_completion)}"
    return None


def build_control_annotation_text(
    ir: IRPlaybook,
    step: IRStep,
    include_condition: bool = True,
    hidden_post_labels: Optional[List[str]] = None,
    extra_outcome_text: Optional[str] = None,
) -> Optional[str]:
    lines: List[str] = []

    meta_parts: List[str] = []
    if _has_nonempty_value(step.delay):
        meta_parts.append(f"delay={_render_compact(step.delay)}")
    if _has_nonempty_value(step.timeout):
        meta_parts.append(f"timeout={_render_compact(step.timeout)}")
    if _has_nonempty_value(step.markings):
        meta_parts.append("markings=" + _comma_join([str(v) for v in _as_list(step.markings)], limit=4))
    if meta_parts:
        lines.append("META: " + "; ".join(meta_parts))

    data_parts: List[str] = []
    if _has_nonempty_value(step.step_variables):
        data_parts.append("variables=" + _short(_summarize_variable_dict(step.step_variables, limit=6), 220))
    if _has_nonempty_value(step.step_extensions):
        data_parts.append("extensions=" + _short(_summarize_dict_keys(step.step_extensions, ["name", "type", "schema"]), 220))
    if data_parts:
        lines.append("DATA: " + ", ".join(data_parts))

    if include_condition and _has_nonempty_value(step.condition):
        lines.append("COND: " + _short(str(step.condition), 220))

    outcome_parts: List[str] = []
    hidden_post_labels = hidden_post_labels or []

    post_map = [
        ("on_completion", step.on_completion),
        ("on_success", step.on_success),
        ("on_failure", step.on_failure),
    ]
    allowed = set(hidden_post_labels)
    for label, target in post_map:
        if label not in allowed:
            continue
        if isinstance(target, str) and target in ir.steps:
            outcome_parts.append(f"{label} -> {_step_display_ref(ir, target)}")
        elif isinstance(target, list):
            for tgt in target:
                if isinstance(tgt, str) and tgt in ir.steps:
                    outcome_parts.append(f"{label} -> {_step_display_ref(ir, tgt)}")

    one_armed_note = _one_armed_if_outcome_note(ir, step)
    if extra_outcome_text:
        outcome_parts.append(extra_outcome_text)
    if one_armed_note and one_armed_note not in outcome_parts:
        outcome_parts.append(one_armed_note)

    if outcome_parts:
        lines.append("OUTCOME: " + "; ".join(outcome_parts))

    return "\n".join(lines) if lines else None


def export_bpmn_process_xml(ir: IRPlaybook, out_path: str) -> None:
    definitions = ET.Element(
        _bpmn("definitions"),
        {"id": f"Definitions_{uuid.uuid4()}", "targetNamespace": "https://example.org/cacao-bpmn"},
    )

    process_id = f"Process_{ir.playbook_id.replace('--', '_')}"
    process = ET.SubElement(
        definitions,
        _bpmn("process"),
        {"id": process_id, "name": ir.name or "CACAO Playbook", "isExecutable": "false"},
    )
    _add_extension_properties(process, {
        "playbook_id": ir.playbook_id,
        "spec_version": ir.spec_version,
        "playbook_types": ir.playbook_types,
        "playbook_activities": ir.playbook_activities,
        "playbook_processing_summary": ir.playbook_processing_summary,
        "workflow_start": ir.workflow_start,
        "created": ir.created,
        "modified": ir.modified,
        "created_by": ir.created_by,
        "valid_from": ir.valid_from,
        "valid_until": ir.valid_until,
        "revoked": ir.revoked,
        "derived_from": ir.derived_from,
        "labels": ir.labels,
        "markings": ir.markings,
        "industry_sectors": ir.industry_sectors,
    })

    #First create semantically compiled nodes and flows
    node_kind: Dict[str, str] = {}  #events, tasks, gateways
    node_original_step: Dict[str, str] = {}

    step_to_bpmn: Dict[str, str] = {}
    control_completion_node: Dict[str, str] = {}
    completion_enabled_controls: Set[str] = set()

    sequence_flows: List[Tuple[str, str, str, str]] = []
    seen_flow_keys: Set[Tuple[str, str, str]] = set()

    compiled_states: Set[Tuple[str, Tuple[str, ...]]] = set()
    compiling_states: Set[Tuple[str, Tuple[str, ...]]] = set()

    raw_incoming_counts: Dict[str, int] = defaultdict(int)
    for _sid, _step in ir.steps.items():
        for _lbl, _tgt in iter_transitions(_step):
            if isinstance(_tgt, str) and _tgt in ir.steps:
                raw_incoming_counts[_tgt] += 1

    def _safe_id_fragment(value: str) -> str:
        return value.replace("--", "_").replace("-", "_")

    def _add_original_bpmn_node(step_id: str) -> str:
        existing = step_to_bpmn.get(step_id)
        if existing:
            return existing

        step = ir.steps[step_id]
        sid = _safe_id_fragment(step_id)

        if step.step_type == "start":
            bpmn_id = f"StartEvent_{sid}"
            el = ET.SubElement(process, _bpmn("startEvent"), {"id": bpmn_id, "name": step.name or "Start"})
            kind = "event"
        elif step.step_type == "end":
            bpmn_id = f"EndEvent_{sid}"
            el = ET.SubElement(process, _bpmn("endEvent"), {"id": bpmn_id, "name": step.name or "End"})
            kind = "event"
        elif step.step_type in ("if-condition", "switch-condition", "while-condition"):
            bpmn_id = f"Gateway_{sid}"
            el = ET.SubElement(process, _bpmn("exclusiveGateway"), {"id": bpmn_id, "name": step.name or step.step_type})
            kind = "gateway"
        elif step.step_type == "parallel":
            bpmn_id = f"Gateway_{sid}"
            el = ET.SubElement(process, _bpmn("parallelGateway"), {"id": bpmn_id, "name": step.name or "Parallel"})
            kind = "gateway"
        else:
            bpmn_id = f"Task_{sid}"
            el = ET.SubElement(process, _bpmn("task"), {"id": bpmn_id, "name": step.name or step.step_type})
            kind = "task"

        if step.description:
            ET.SubElement(el, _bpmn("documentation")).text = step.description

        _add_extension_properties(el, {
            "cacao_step_id": step.step_id,
            "cacao_step_type": step.step_type,
            "delay": step.delay,
            "timeout": step.timeout,
            "in_args": step.in_args,
            "out_args": step.out_args,
            "step_variables": list(step.step_variables.keys()) if isinstance(step.step_variables, dict) else None,
            "markings": step.markings,
            "step_extensions": list(step.step_extensions.keys()) if isinstance(step.step_extensions, dict) else None,
            "agent": step.agent,
            "targets": step.targets,
            "playbook_id": step.playbook_id,
            "playbook_version": step.playbook_version,
        })

        step_to_bpmn[step_id] = bpmn_id
        node_kind[bpmn_id] = kind
        node_original_step[bpmn_id] = step_id
        return bpmn_id

    def _ensure_completion_node(step_id: str) -> str:
        existing = control_completion_node.get(step_id)
        if existing:
            return existing

        step = ir.steps[step_id]
        sid = _safe_id_fragment(step_id)

        if step.step_type == "parallel":
            bpmn_id = f"GatewayJoin_{sid}"
            el = ET.SubElement(process, _bpmn("parallelGateway"), {"id": bpmn_id})
            doc_text = f"Synthetic join gateway for parallel step {step.name or step.step_type}."
        else:
            bpmn_id = f"GatewayMerge_{sid}"
            el = ET.SubElement(process, _bpmn("exclusiveGateway"), {"id": bpmn_id})
            doc_text = f"Synthetic merge gateway for {step.name or step.step_type}."

        if step.description:
            doc_text += f" {step.description}"
        ET.SubElement(el, _bpmn("documentation")).text = doc_text
        _add_extension_properties(el, {
            "synthetic_for_step": step.step_id,
            "synthetic_for_type": step.step_type,
            "synthetic_kind": "parallel_join" if step.step_type == "parallel" else "xor_merge",
        })

        control_completion_node[step_id] = bpmn_id
        node_kind[bpmn_id] = "gateway"
        node_original_step[bpmn_id] = step_id
        return bpmn_id

    def _add_sequence_flow(src_bpmn: str, tgt_bpmn: str, label: str) -> None:
        if not src_bpmn or not tgt_bpmn or src_bpmn == tgt_bpmn:
            return
        flow_key = (src_bpmn, tgt_bpmn, label)
        if flow_key in seen_flow_keys:
            return
        seen_flow_keys.add(flow_key)
        flow_id = f"Flow_{uuid.uuid4().hex}"
        flow_attrs = {"id": flow_id, "sourceRef": src_bpmn, "targetRef": tgt_bpmn}
        flow_name = _flow_label_text(label)
        if flow_name:
            flow_attrs["name"] = flow_name
        ET.SubElement(process, _bpmn("sequenceFlow"), flow_attrs)
        sequence_flows.append((flow_id, src_bpmn, tgt_bpmn, label))

    def _control_branch_transitions(step: IRStep) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []

        def add(label: str, target):
            if isinstance(target, str):
                out.append((label, target))
            elif isinstance(target, list):
                for t in target:
                    if isinstance(t, str):
                        out.append((label, t))

        if step.step_type in ("if-condition", "while-condition"):
            add("true", step.on_true)
            add("false", step.on_false)
        elif step.step_type == "switch-condition":
            if isinstance(step.cases, dict):
                for case_key, target in step.cases.items():
                    add(f"case:{case_key}", target)
            add("default", step.default)
        elif step.step_type == "parallel":
            add("next_step", step.next_steps)
        return out

    def _control_post_transitions(step: IRStep) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []

        def add(label: str, target):
            if isinstance(target, str):
                out.append((label, target))
            elif isinstance(target, list):
                for t in target:
                    if isinstance(t, str):
                        out.append((label, t))

        add("on_completion", step.on_completion)
        add("on_success", step.on_success)
        add("on_failure", step.on_failure)
        return out

    def _post_target_ids(step: IRStep) -> Set[str]:
        return {t for _lbl, t in _control_post_transitions(step) if t in ir.steps}

    def _branch_target_ids(step: IRStep) -> Set[str]:
        return {t for _lbl, t in _control_branch_transitions(step) if t in ir.steps}

    def _linear_local_end(start_id: str) -> Optional[str]:
        current = start_id
        seen_local: Set[str] = set()
        first = True

        while current in ir.steps:
            if current in seen_local:
                return None
            seen_local.add(current)

            step = ir.steps[current]
            if step.step_type == "end":
                return current

            if step.step_type in ("start", "if-condition", "switch-condition", "while-condition", "parallel"):
                return None

            if not first and raw_incoming_counts.get(current, 0) > 1:
                return None

            trans = [(lbl, tgt) for lbl, tgt in iter_transitions(step) if isinstance(tgt, str) and tgt in ir.steps]
            on_completion = [(lbl, tgt) for lbl, tgt in trans if lbl == "on_completion"]
            others = [(lbl, tgt) for lbl, tgt in trans if lbl != "on_completion"]

            if others or len(on_completion) != 1:
                return None

            current = on_completion[0][1]
            first = False

        return None

    def _should_create_xor_merge(step: IRStep) -> bool:
        if step.step_type not in ("if-condition", "switch-condition"):
            return False

        branches = _control_branch_transitions(step)
        if step.step_type == "if-condition":
            if not (_has_nonempty_value(step.on_true) and _has_nonempty_value(step.on_false)):
                return False
        if len(branches) < 2:
            return False

        branch_targets = _branch_target_ids(step)
        distinct_post_targets = [t for _lbl, t in _control_post_transitions(step) if t in ir.steps and t not in branch_targets]
        if not distinct_post_targets:
            return False

        for _lbl, tgt in branches:
            if tgt not in ir.steps:
                return False
            if _linear_local_end(tgt) is None:
                return False
        return True

    def _should_create_parallel_join(step: IRStep) -> bool:
        if step.step_type != "parallel":
            return False

        branches = _control_branch_transitions(step)
        if len(branches) < 2:
            return False

        distinct_post_targets = [t for _lbl, t in _control_post_transitions(step) if t in ir.steps]
        if not distinct_post_targets:
            return False

        for _lbl, tgt in branches:
            if tgt not in ir.steps:
                return False
            if _linear_local_end(tgt) is None:
                return False
        return True

    def _completion_enabled(step: IRStep) -> bool:
        if step.step_type == "parallel":
            return _should_create_parallel_join(step)
        if step.step_type in ("if-condition", "switch-condition"):
            return _should_create_xor_merge(step)
        return False

    hidden_post_labels_by_step: Dict[str, List[str]] = {}

    def _compile_entry(step_id: str, ctx: Tuple[str, ...]) -> str:
        step = ir.steps[step_id]
        if step.step_type == "end" and ctx:
            active_control_id = ctx[-1]
            active_step = ir.steps[active_control_id]
            if (
                active_control_id in completion_enabled_controls
                and step_id not in _post_target_ids(active_step)
                and not (ir.workflow_exception and step_id == ir.workflow_exception)
            ):
                hidden_post_labels_by_step.setdefault(active_control_id, [])
                return _ensure_completion_node(active_control_id)
        return _compile_step(step_id, ctx)

    def _compile_step(step_id: str, ctx: Tuple[str, ...]) -> str:
        step = ir.steps[step_id]
        src_bpmn = _add_original_bpmn_node(step_id)

        state = (step_id, ctx)
        if state in compiled_states or state in compiling_states:
            return src_bpmn

        compiling_states.add(state)

        if step.step_type == "parallel":
            if _completion_enabled(step):
                completion_enabled_controls.add(step_id)
                branch_ctx = ctx + (step_id,)
            else:
                branch_ctx = ctx

            for label, tgt_step_id in _control_branch_transitions(step):
                if tgt_step_id in ir.steps:
                    tgt_bpmn = _compile_entry(tgt_step_id, branch_ctx)
                    _add_sequence_flow(src_bpmn, tgt_bpmn, label)

            if step_id in completion_enabled_controls:
                completion_bpmn = _ensure_completion_node(step_id)
                for label, tgt_step_id in _control_post_transitions(step):
                    if tgt_step_id in ir.steps:
                        tgt_bpmn = _compile_entry(tgt_step_id, ctx)
                        _add_sequence_flow(completion_bpmn, tgt_bpmn, label)
            else:
                if _control_post_transitions(step):
                    hidden_post_labels_by_step[step_id] = [lbl for lbl, _t in _control_post_transitions(step)]

        elif step.step_type in ("if-condition", "switch-condition", "while-condition"):
            if _completion_enabled(step):
                completion_enabled_controls.add(step_id)
                branch_ctx = ctx + (step_id,)
            else:
                branch_ctx = ctx

            for label, tgt_step_id in _control_branch_transitions(step):
                if tgt_step_id in ir.steps:
                    tgt_bpmn = _compile_entry(tgt_step_id, branch_ctx)
                    _add_sequence_flow(src_bpmn, tgt_bpmn, label)

            if step_id in completion_enabled_controls:
                completion_bpmn = _ensure_completion_node(step_id)
                branch_targets = _branch_target_ids(step)
                for label, tgt_step_id in _control_post_transitions(step):
                    if tgt_step_id in ir.steps and tgt_step_id not in branch_targets:
                        tgt_bpmn = _compile_entry(tgt_step_id, ctx)
                        _add_sequence_flow(completion_bpmn, tgt_bpmn, label)
            else:
                if _control_post_transitions(step):
                    hidden_post_labels_by_step[step_id] = [lbl for lbl, _t in _control_post_transitions(step)]

        else:
            for label, tgt_step_id in iter_transitions(step):
                if not isinstance(tgt_step_id, str) or tgt_step_id not in ir.steps:
                    continue
                tgt_bpmn = _compile_entry(tgt_step_id, ctx)
                _add_sequence_flow(src_bpmn, tgt_bpmn, label)

        compiling_states.remove(state)
        compiled_states.add(state)
        return src_bpmn

    start_bpmn = _compile_step(ir.workflow_start, ())

    def _norm_step_ids(value) -> List[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return [v for v in value if isinstance(v, str)]
        return []

    def _remove_sequence_flows_between(src_bpmn: str, tgt_bpmn: str) -> List[str]:
        removed_labels: List[str] = []
        kept: List[Tuple[str, str, str, str]] = []
        for flow_id, s, t, lbl in sequence_flows:
            if s == src_bpmn and t == tgt_bpmn:
                removed_labels.append(lbl)
                flow_el = process.find(f".//{{{BPMN_NS}}}sequenceFlow[@id='{flow_id}']")
                if flow_el is not None:
                    process.remove(flow_el)
                seen_flow_keys.discard((s, t, lbl))
            else:
                kept.append((flow_id, s, t, lbl))
        sequence_flows[:] = kept
        return removed_labels

    def _branch_completion_candidates(step_id: str) -> Dict[str, Set[str]]:
        out: Dict[str, Set[str]] = defaultdict(set)
        seen: Set[str] = set()

        def visit(sid: str):
            if sid in seen or sid not in ir.steps:
                return
            seen.add(sid)
            step = ir.steps[sid]
            if step.step_type in ("if-condition", "while-condition"):
                for target in _norm_step_ids(step.on_true) + _norm_step_ids(step.on_false):
                    visit(target)
                return
            if step.step_type == "switch-condition":
                if isinstance(step.cases, dict):
                    for target in step.cases.values():
                        for tid in _norm_step_ids(target):
                            visit(tid)
                for tid in _norm_step_ids(step.default):
                    visit(tid)
                return
            if step.step_type == "parallel":
                for tid in _norm_step_ids(step.next_steps):
                    visit(tid)
                return
            if step.step_type in ("start", "end"):
                return
            for tid in _norm_step_ids(step.on_completion):
                if tid in ir.steps:
                    out[tid].add(sid)

        visit(step_id)
        return out

    def _apply_parallel_shared_successor_joins() -> None:
        for control_step_id, step in ir.steps.items():
            if step.step_type != "parallel":
                continue
            branch_roots = [t for _lbl, t in _control_branch_transitions(step) if t in ir.steps]
            if len(branch_roots) < 2:
                continue
            per_branch = [_branch_completion_candidates(root) for root in branch_roots]
            if any(not m for m in per_branch):
                continue
            common_targets = set(per_branch[0].keys())
            for m in per_branch[1:]:
                common_targets &= set(m.keys())
            common_targets = {t for t in common_targets if t in ir.steps and ir.steps[t].step_type != "end" and t not in branch_roots}
            if len(common_targets) != 1:
                continue
            target_step_id = next(iter(common_targets))
            target_bpmn = step_to_bpmn.get(target_step_id)
            if not target_bpmn:
                continue
            src_step_ids: List[str] = []
            for m in per_branch:
                srcs = sorted(m.get(target_step_id, set()))
                if not srcs:
                    src_step_ids = []
                    break
                src_step_ids.extend(srcs)
            src_step_ids = sorted(set(src_step_ids))
            if len(src_step_ids) < 2:
                continue

            join_bpmn = _ensure_completion_node(control_step_id)
            rewired = False
            for src_step_id in src_step_ids:
                src_bpmn = step_to_bpmn.get(src_step_id)
                if not src_bpmn:
                    continue
                removed_labels = _remove_sequence_flows_between(src_bpmn, target_bpmn)
                if removed_labels:
                    rewired = True
                    _add_sequence_flow(src_bpmn, join_bpmn, "")
            if rewired:
                _add_sequence_flow(join_bpmn, target_bpmn, "")

    _apply_parallel_shared_successor_joins()

    label_rank_offset_for_flow: Dict[str, int] = {}
    grouped_label_flows = defaultdict(list)
    labeled_flow_ids_by_source: Dict[str, List[str]] = defaultdict(list)
    for _flow_id, src_bpmn, tgt_bpmn, lbl in sequence_flows:
        if _flow_label_text(lbl):
            grouped_label_flows[src_bpmn].append((_flow_id, tgt_bpmn))
            labeled_flow_ids_by_source[src_bpmn].append(_flow_id)

    #Rank sibling labels by target position to reduce overlap

    #Diagram interchange plane
    diagram = ET.SubElement(definitions, _bpmndi("BPMNDiagram"), {"id": "BPMNDiagram_1"})
    plane = ET.SubElement(diagram, _bpmndi("BPMNPlane"), {"id": "BPMNPlane_1", "bpmnElement": process_id})

    #Layout layers
    adj: Dict[str, List[str]] = defaultdict(list)
    for _, src, tgt, _ in sequence_flows:
        adj[src].append(tgt)

    layer: Dict[str, int] = {start_bpmn: 0}
    q = deque([start_bpmn])
    seen: Set[str] = {start_bpmn}
    while q:
        u = q.popleft()
        for v in adj.get(u, []):
            if v not in layer:
                layer[v] = layer[u] + 1
            if v not in seen:
                seen.add(v)
                q.append(v)

    max_layer = max(layer.values()) if layer else 0
    for nid in node_kind.keys():
        if nid not in layer:
            max_layer += 1
            layer[nid] = max_layer

    nodes_by_layer: Dict[int, List[str]] = defaultdict(list)
    for nid, l in layer.items():
        nodes_by_layer[l].append(nid)
    for l in nodes_by_layer:
        nodes_by_layer[l].sort()

    #Geometries
    X0, Y0 = 100, 120
    GAP_X, GAP_Y = 280, 150
    EVENT_W, EVENT_H = 36, 36
    TASK_W, TASK_H = 160, 90
    GW_W, GW_H = 50, 50

    bounds: Dict[str, Tuple[int, int, int, int]] = {}
    shape_bounds_el: Dict[str, ET.Element] = {}

    def add_shape(bpmn_element_id: str, x: int, y: int, w: int, h: int, shape_id: str):
        shape = ET.SubElement(plane, _bpmndi("BPMNShape"), {"id": shape_id, "bpmnElement": bpmn_element_id})
        b = ET.SubElement(shape, _dc("Bounds"), {"x": str(x), "y": str(y), "width": str(w), "height": str(h)})
        bounds[bpmn_element_id] = (x, y, w, h)
        shape_bounds_el[bpmn_element_id] = b

    #Node shapes
    sc = 0
    for l in sorted(nodes_by_layer.keys()):
        for idx, nid in enumerate(nodes_by_layer[l]):
            x = X0 + l * GAP_X
            y = Y0 + idx * GAP_Y
            kind = node_kind.get(nid, "task")
            if kind == "event":
                add_shape(nid, x, y, EVENT_W, EVENT_H, f"BPMNShape_{sc}")
            elif kind == "gateway":
                add_shape(nid, x, y, GW_W, GW_H, f"BPMNShape_{sc}")
            else:
                add_shape(nid, x, y, TASK_W, TASK_H, f"BPMNShape_{sc}")
            sc += 1

    #Optional separate workflow_exception endpoint on the playbook level
    exception_bpmn_id = None
    exception_step = None
    if ir.workflow_exception and ir.workflow_exception in ir.steps:
        exception_step = ir.steps[ir.workflow_exception]
        if exception_step.step_type == "end":
            exception_bpmn_id = _add_original_bpmn_node(ir.workflow_exception)
            if exception_bpmn_id not in bounds:
                max_y = max((y + h for (_x, y, _w, h) in bounds.values()), default=Y0)
                add_shape(exception_bpmn_id, X0, max_y + 180, EVENT_W, EVENT_H, f"BPMNShape_{sc}")
                sc += 1

    def _rects_overlap_basic(r1, r2) -> bool:
        x1, y1, w1, h1 = r1
        x2, y2, w2, h2 = r2
        return not (x1 + w1 <= x2 or x2 + w2 <= x1 or y1 + h1 <= y2 or y2 + h2 <= y1)

    def _median(values: List[float]) -> float:
        vals = sorted(values)
        n = len(vals)
        if n == 0:
            return 0.0
        if n % 2 == 1:
            return vals[n // 2]
        return (vals[n // 2 - 1] + vals[n // 2]) / 2.0

    def _reposition_convergence_tasks() -> None:
        incoming_by_target: Dict[str, List[str]] = defaultdict(list)
        for _fid, src, tgt, _lbl in sequence_flows:
            incoming_by_target[tgt].append(src)

        for tgt, preds in incoming_by_target.items():
            if node_kind.get(tgt) != "task":
                continue
            later_preds = [p for p in preds if layer.get(p, 0) > layer.get(tgt, 0)]
            if len(preds) < 3 or len(later_preds) < 2:
                continue
            if tgt not in bounds:
                continue

            x, y, w, h = bounds[tgt]
            pred_centers_y = [bounds[p][1] + bounds[p][3] / 2.0 for p in preds if p in bounds]
            if not pred_centers_y:
                continue

            desired_y = int(round(_median(pred_centers_y) - h / 2.0))

            next_layer_nodes = nodes_by_layer.get(layer.get(tgt, 0) + 1, [])
            next_layer_min_x = min((bounds[n][0] for n in next_layer_nodes if n in bounds), default=x + w + 160)
            max_shift_x = max(0, min(80, int(next_layer_min_x - (x + w) - 60)))
            candidate_xs = [x + max_shift_x, x + max_shift_x // 2, x]
            candidate_xs = list(dict.fromkeys(candidate_xs))

            candidate_ys = [desired_y, desired_y - 30, desired_y + 30, y, desired_y - 60, desired_y + 60]
            candidate_ys = [max(40, cy) for cy in candidate_ys]

            best = None
            best_score = None
            for cx in candidate_xs:
                for cy in candidate_ys:
                    rect = (cx, cy, w, h)
                    collision = False
                    for oid, orect in bounds.items():
                        if oid == tgt:
                            continue
                        if _rects_overlap_basic(rect, orect):
                            collision = True
                            break
                    if collision:
                        continue

                    center_x = cx + w / 2.0
                    center_y = cy + h / 2.0
                    score = 0.0
                    for p in preds:
                        if p not in bounds:
                            continue
                        px, py, pw, ph = bounds[p]
                        pcy = py + ph / 2.0
                        pcx = px + pw / 2.0
                        score += abs(center_y - pcy) + 0.15 * abs(center_x - pcx)
                    score += 0.5 * abs(cy - y) + 0.25 * abs(cx - x)

                    if best is None or score < best_score:
                        best = (cx, cy)
                        best_score = score

            if best is not None and best != (x, y):
                nx, ny = best
                bounds[tgt] = (nx, ny, w, h)
                b = shape_bounds_el.get(tgt)
                if b is not None:
                    b.set("x", str(nx))
                    b.set("y", str(ny))

    _reposition_convergence_tasks()

    #Make routing ignore annotation boxes
    node_bounds = dict(bounds)

    for src_bpmn, items in grouped_label_flows.items():
        items.sort(key=lambda ft: node_bounds[ft[1]][1] + node_bounds[ft[1]][3] // 2)
        mid = (len(items) - 1) / 2.0
        for idx, (flow_id_for_rank, _tgt_bpmn) in enumerate(items):
            label_rank_offset_for_flow[flow_id_for_rank] = int(round((idx - mid) * 30))

    #Helper functions for collisions
    def _rects_overlap(r1, r2) -> bool:
        x1, y1, w1, h1 = r1
        x2, y2, w2, h2 = r2
        return not (x1 + w1 <= x2 or x2 + w2 <= x1 or y1 + h1 <= y2 or y2 + h2 <= y1)
    
    def _inflate_rect_for_annotation_clearance(elem_id: str, rect: Tuple[int, int, int, int]) -> Tuple[int, int, int, int]:
        x, y, w, h = rect

        kind = node_kind.get(elem_id, None)

        #Avoid Events/Gateways
        if kind == "event":
            pad_x, pad_y = 70, 50
        elif kind == "gateway":
            pad_x, pad_y = 55, 40
        elif kind == "task":
            pad_x, pad_y = 18, 14
        else:
            #Avoid annotations boxes
            pad_x, pad_y = 16, 12

        return (x - pad_x, y - pad_y, w + 2 * pad_x, h + 2 * pad_y)

    def _collides(candidate_rect, bounds_dict) -> bool:
        for _id, rect in bounds_dict.items():
            inflated = _inflate_rect_for_annotation_clearance(_id, rect)
            if _rects_overlap(candidate_rect, inflated):
                return True
        return False

    #Create a fixed annotation column on the right
    max_right = max(x + w for (x, y, w, h) in node_bounds.values()) if node_bounds else X0
    ANN_X = max_right + 140
    ANN_X_2 = ANN_X + 280

    def place_annotation_in_column(anchor_id: str, w: int, h: int) -> Tuple[int, int, int, int]:
        ax, ay, aw, ah = node_bounds[anchor_id]

        candidate_xs = [
            ax + aw + 90,
            ax + aw + 240,
            ANN_X,
            ANN_X_2,
        ]
        candidate_xs = list(dict.fromkeys(int(v) for v in candidate_xs))

        gap = h + 44
        candidate_ys = [
            ay,
            ay - gap,
            ay + (ah + 44),
            ay - 2 * gap,
            ay + 2 * gap,
            ay - 3 * gap,
            ay + 3 * gap,
        ]

        best_rect = None
        best_score = None

        for x in candidate_xs:
            for base_y in candidate_ys:
                y = max(40, base_y)
                for _ in range(90):
                    rect = (x, y, w, h)
                    if not _collides(rect, bounds):
                        score = abs(x - (ax + aw)) + abs(y - ay) + (40 if x == ANN_X_2 else 0)
                        if best_rect is None or score < best_score:
                            best_rect = rect
                            best_score = score
                        break
                    y += 24

        if best_rect is not None:
            return best_rect

        return (ANN_X_2, max(40, ay), w, h)

    #Create annotation elements
    def add_text_annotation(text: str) -> str:
        ann_id = f"TextAnnotation_{uuid.uuid4().hex}"
        ann = ET.SubElement(process, _bpmn("textAnnotation"), {"id": ann_id})
        ET.SubElement(ann, _bpmn("text")).text = text
        return ann_id

    def add_association(src_bpmn_id: str, ann_id: str) -> str:
        assoc_id = f"Association_{uuid.uuid4().hex}"
        ET.SubElement(process, _bpmn("association"), {"id": assoc_id, "sourceRef": src_bpmn_id, "targetRef": ann_id})
        return assoc_id

    def add_annotation_shape(ann_id: str, x: int, y: int, w: int, h: int):
        shape = ET.SubElement(plane, _bpmndi("BPMNShape"), {"id": f"BPMNShape_{ann_id}", "bpmnElement": ann_id})
        ET.SubElement(shape, _dc("Bounds"), {"x": str(x), "y": str(y), "width": str(w), "height": str(h)})
        bounds[ann_id] = (x, y, w, h)

    #Routing helpers to avoid crossing node shapes
    def _segment_hits_rect(p, q, rect) -> bool:
        x, y, w, h = rect
        x1, y1 = p
        x2, y2 = q
        #Vertically
        if x1 == x2:
            if x1 < x or x1 > x + w:
                return False
            ymin, ymax = (y1, y2) if y1 <= y2 else (y2, y1)
            return not (ymax < y or ymin > y + h)
        #Horizontally
        if y1 == y2:
            if y1 < y or y1 > y + h:
                return False
            xmin, xmax = (x1, x2) if x1 <= x2 else (x2, x1)
            return not (xmax < x or xmin > x + w)
        return False

    def _polyline_hits_nodes(points: List[Tuple[int, int]], src_id: str, tgt_id: str) -> bool:
        for bid, rect in node_bounds.items():
            if bid in (src_id, tgt_id):
                continue
            for i in range(len(points) - 1):
                if _segment_hits_rect(points[i], points[i + 1], rect):
                    return True
        return False

    def _polyline_hits_rects(points: List[Tuple[int, int]], rects: Dict[str, Tuple[int, int, int, int]], skip: Set[str]) -> bool:
        for rid, rect in rects.items():
            if rid in skip:
                continue
            for i in range(len(points) - 1):
                if _segment_hits_rect(points[i], points[i + 1], rect):
                    return True
        return False

    def _polyline_length(points: List[Tuple[int, int]]) -> int:
        total = 0
        for i in range(len(points) - 1):
            total += abs(points[i + 1][0] - points[i][0]) + abs(points[i + 1][1] - points[i][1])
        return total

    def _annotation_hits(points: List[Tuple[int, int]], skip: Set[str]) -> int:
        hits = 0
        for rid, rect in bounds.items():
            if rid in skip or rid in node_bounds:
                continue
            for i in range(len(points) - 1):
                if _segment_hits_rect(points[i], points[i + 1], rect):
                    hits += 1
                    break
        return hits

    _assoc_src_counter = defaultdict(int)

    def _spread_port_y(y: int, h: int, idx: int) -> int:
        offsets = [0, -12, 12, -24, 24, -36, 36]
        off = offsets[idx] if idx < len(offsets) else ((idx - len(offsets) + 1) * 12)
        return max(y + 12, min(y + h - 12, y + h // 2 + off))

    #Association edge to create local candidate corridors and avoid all boxes
    def add_association_edge(assoc_id: str, src_id: str, ann_id: str):
        if src_id not in bounds or ann_id not in bounds:
            return

        sx, sy, sw, sh = bounds[src_id]
        ax, ay, aw, ah = bounds[ann_id]

        src_slot = _assoc_src_counter[src_id]
        _assoc_src_counter[src_id] += 1

        src_point = (sx + sw, _spread_port_y(sy, sh, src_slot))
        tgt_point = (ax, ay + ah // 2)

        candidate_mid_xs = [
            sx + sw + 30,
            sx + sw + 80,
            min(ax - 30, sx + sw + 140),
            ax - 30,
            ax + aw + 30,
        ]

        seen = set()
        candidate_mid_xs = [mx for mx in candidate_mid_xs if mx > src_point[0] and not (mx in seen or seen.add(mx))]

        def polyline_length(points: List[Tuple[int, int]]) -> int:
            total = 0
            for i in range(len(points) - 1):
                total += abs(points[i + 1][0] - points[i][0]) + abs(points[i + 1][1] - points[i][1])
            return total

        best_pts = None
        best_len = None

        for mid_x in candidate_mid_xs:
            pts = [
                src_point,
                (mid_x, src_point[1]),
                (mid_x, tgt_point[1]),
                tgt_point,
            ]
            if not _polyline_hits_rects(pts, bounds, {src_id, ann_id}):
                plen = polyline_length(pts)
                if best_pts is None or plen < best_len:
                    best_pts = pts
                    best_len = plen

        if best_pts is None:
            fallback_x = max(sx + sw + 80, ax + 40)
            best_pts = [
                src_point,
                (fallback_x, src_point[1]),
                (fallback_x, tgt_point[1]),
                tgt_point,
            ]

        edge = ET.SubElement(plane, _bpmndi("BPMNEdge"), {"id": f"BPMNEdge_{assoc_id}", "bpmnElement": assoc_id})
        for x, y in best_pts:
            ET.SubElement(edge, _di("waypoint"), {"x": str(int(x)), "y": str(int(y))})

    #META annotation tag at start
    meta_text = build_meta_text(ir)
    meta_text, meta_w, meta_h = _prepare_annotation_box(meta_text, min_width=460, max_width=760)
    meta_ann_id = add_text_annotation(meta_text)
    meta_assoc_id = add_association(start_bpmn, meta_ann_id)
    x, y, w, h = place_annotation_in_column(start_bpmn, meta_w, meta_h)
    add_annotation_shape(meta_ann_id, x, y, w, h)
    add_association_edge(meta_assoc_id, start_bpmn, meta_ann_id)

    #DATA/ROLE tags on task-like steps with real metadata
    for task_bpmn_id, step_id in node_original_step.items():
        if node_kind.get(task_bpmn_id) != "task":
            continue
        if task_bpmn_id not in node_bounds:
            continue
        step = ir.steps.get(step_id)
        if not step or not is_task_like_step(step):
            continue
        if not step_has_annotation_metadata(step):
            continue
        annotation_text = build_task_annotation_text(ir, step)
        if not annotation_text:
            continue
        annotation_text, ann_width, ann_height = _prepare_annotation_box(annotation_text, min_width=460, max_width=760)
        ann_id = add_text_annotation(annotation_text)
        assoc_id = add_association(task_bpmn_id, ann_id)
        x, y, w, h = place_annotation_in_column(task_bpmn_id, ann_width, ann_height)
        add_annotation_shape(ann_id, x, y, w, h)
        add_association_edge(assoc_id, task_bpmn_id, ann_id)


    #COND/OUTCOME tags on gateways where they are relevant
    for gateway_bpmn_id, step_id in node_original_step.items():
        if node_kind.get(gateway_bpmn_id) != "gateway":
            continue
        if gateway_bpmn_id not in node_bounds:
            continue

        step = ir.steps.get(step_id)
        if not step:
            continue

        if gateway_bpmn_id != step_to_bpmn.get(step_id):
            continue

        hidden_labels = hidden_post_labels_by_step.get(step_id, [])

        annotation_text = build_control_annotation_text(
            ir,
            step,
            include_condition=(step.step_type in ("if-condition", "while-condition")),
            hidden_post_labels=hidden_labels,
            extra_outcome_text=None,
        )
        if not annotation_text:
            continue

        annotation_text, ann_width, ann_height = _prepare_annotation_box(annotation_text, min_width=440, max_width=720)

        ann_id = add_text_annotation(annotation_text)
        assoc_id = add_association(gateway_bpmn_id, ann_id)
        x, y, w, h = place_annotation_in_column(gateway_bpmn_id, ann_width, ann_height)
        add_annotation_shape(ann_id, x, y, w, h)
        add_association_edge(assoc_id, gateway_bpmn_id, ann_id)

    #Create an explicit annotation for a synthetic parallel join gateway
    for control_step_id, completion_bpmn_id in control_completion_node.items():
        step = ir.steps.get(control_step_id)
        if not step or step.step_type != "parallel":
            continue
        if completion_bpmn_id not in node_bounds:
            continue
        join_text = f"OUTCOME: parallel completion for {control_step_id}"
        join_text, ann_width, ann_height = _prepare_annotation_box(join_text, min_width=420, max_width=620)
        ann_id = add_text_annotation(join_text)
        assoc_id = add_association(completion_bpmn_id, ann_id)
        x, y, w, h = place_annotation_in_column(completion_bpmn_id, ann_width, ann_height)
        add_annotation_shape(ann_id, x, y, w, h)
        add_association_edge(assoc_id, completion_bpmn_id, ann_id)

    #Create an explicit annotation for separate workflow_exception endpoints
    if exception_bpmn_id and exception_bpmn_id in node_bounds:
        exception_text = "OUTCOME: workflow_exception endpoint"
        exception_text, ann_width, ann_height = _prepare_annotation_box(exception_text, min_width=400, max_width=560)
        ann_id = add_text_annotation(exception_text)
        assoc_id = add_association(exception_bpmn_id, ann_id)
        x, y, w, h = place_annotation_in_column(exception_bpmn_id, ann_width, ann_height)
        add_annotation_shape(ann_id, x, y, w, h)
        add_association_edge(assoc_id, exception_bpmn_id, ann_id)

    #Create sequence flow edges
    _flow_out_counter = defaultdict(int)
    _flow_in_counter = defaultdict(int)

    def _task_port_y(y: int, h: int, idx: int) -> int:
        offsets = [0, -12, 12, -24, 24, -36, 36]
        off = offsets[idx] if idx < len(offsets) else ((idx - len(offsets) + 1) * 12)
        return max(y + 12, min(y + h - 12, y + h // 2 + off))

    def _expand_rect_for_flow(elem_id: str, rect: Tuple[int, int, int, int]) -> Tuple[int, int, int, int]:
        x, y, w, h = rect
        kind = node_kind.get(elem_id, None)
        if kind == 'task':
            pad_x, pad_y = 18, 14
        elif kind == 'gateway':
            pad_x, pad_y = 22, 18
        elif kind == 'event':
            pad_x, pad_y = 20, 16
        else:
            #Lighter padding for annotation boxes
            pad_x, pad_y = 10, 8
        return (x - pad_x, y - pad_y, w + 2 * pad_x, h + 2 * pad_y)

    def _point_in_rect(pt: Tuple[int, int], rect: Tuple[int, int, int, int]) -> bool:
        x, y = pt
        rx, ry, rw, rh = rect
        return rx <= x <= rx + rw and ry <= y <= ry + rh

    def _segment_hits_rect_expanded(p: Tuple[int, int], q: Tuple[int, int], elem_id: str, rect: Tuple[int, int, int, int]) -> bool:
        return _segment_hits_rect(p, q, _expand_rect_for_flow(elem_id, rect))

    def _segment_clear_flow(p: Tuple[int, int], q: Tuple[int, int], skip: Set[str]) -> bool:
        for rid, rect in bounds.items():
            if rid in skip:
                continue
            if _segment_hits_rect_expanded(p, q, rid, rect):
                return False
        return True

    def _polyline_hits_flow_obstacles(points: List[Tuple[int, int]], skip: Set[str]) -> bool:
        for i in range(len(points) - 1):
            if not _segment_clear_flow(points[i], points[i + 1], skip):
                return True
        return False

    def _compress_polyline(points: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
        if len(points) <= 2:
            return points
        out = [points[0]]
        for i in range(1, len(points) - 1):
            a = out[-1]
            b = points[i]
            c = points[i + 1]
            if (a[0] == b[0] == c[0]) or (a[1] == b[1] == c[1]):
                continue
            out.append(b)
        out.append(points[-1])
        return out

    def _nearest(values: List[int], target: int, k: int = 3) -> List[int]:
        return [v for _, v in sorted((abs(v - target), v) for v in values)[:k]]

    def _source_port(elem_id: str, target_id: str) -> Tuple[int, int, str]:
        x, y, w, h = node_bounds[elem_id]
        slot = _flow_out_counter[elem_id]
        _flow_out_counter[elem_id] += 1
        py = _task_port_y(y, h, slot)
        kind = node_kind.get(elem_id, 'task')
        tx, ty, tw, th = node_bounds[target_id]
        src_cx = x + w // 2
        tgt_cx = tx + tw // 2

        if kind == 'task':
            return (x + w, py, 'R')
        side = 'R' if tgt_cx >= src_cx else 'L'
        return ((x + w, py, 'R') if side == 'R' else (x, py, 'L'))

    def _target_port(elem_id: str, source_id: str) -> Tuple[int, int, str]:
        x, y, w, h = node_bounds[elem_id]
        slot = _flow_in_counter[elem_id]
        _flow_in_counter[elem_id] += 1
        py = _task_port_y(y, h, slot)
        kind = node_kind.get(elem_id, 'task')
        sx, sy, sw, sh = node_bounds[source_id]
        src_cx = sx + sw // 2
        tgt_cx = x + w // 2

        if kind == 'task':
            return (x, py, 'L')
        side = 'L' if src_cx <= tgt_cx else 'R'
        return ((x, py, 'L') if side == 'L' else (x + w, py, 'R'))

    #Prefer horizontal lanes between rows of node shapes only but not for annotations
    node_center_ys = sorted({node_bounds[nid][1] + node_bounds[nid][3] // 2 for nid in node_bounds if nid in node_kind})
    row_corridors: List[int] = []
    if node_center_ys:
        row_corridors.append(node_center_ys[0] - 70)
        for a, b in zip(node_center_ys, node_center_ys[1:]):
            row_corridors.append((a + b) // 2)
        row_corridors.append(node_center_ys[-1] + 70)

    global_left = min(x for (x, y, w, h) in bounds.values())
    global_right = max(x + w for (x, y, w, h) in bounds.values())

    forward_corridors = {}
    for l in sorted(nodes_by_layer.keys()):
        if (l + 1) in nodes_by_layer:
            right_l = max(node_bounds[nid][0] + node_bounds[nid][2] for nid in nodes_by_layer[l])
            left_next = min(node_bounds[nid][0] for nid in nodes_by_layer[l + 1])
            forward_corridors[(l, l + 1)] = (right_l + left_next) // 2

    def _horizontal_penalty(y: int) -> int:
        if not row_corridors:
            return 0
        d = min(abs(y - rc) for rc in row_corridors)
        return d * 4

    def _x_outer_penalty(x: int) -> int:
        if x < global_left - 20:
            return (global_left - 20 - x) * 2
        if x > global_right + 20:
            return (x - (global_right + 20)) * 2
        return 0

    def _score_path(points: List[Tuple[int, int]]) -> int:
        turns = 0
        for i in range(1, len(points) - 1):
            a, b, c = points[i - 1], points[i], points[i + 1]
            if (a[0] == b[0] and b[1] == c[1]) or (a[1] == b[1] and b[0] == c[0]):
                turns += 1
        score = _polyline_length(points) + turns * 30
        for i in range(len(points) - 1):
            p, q = points[i], points[i + 1]
            if p[1] == q[1]:
                score += _horizontal_penalty(p[1])
            if p[0] == q[0]:
                score += _x_outer_penalty(p[0])
        return score

    def _route_flow_between_stubs(src_stub: Tuple[int, int], tgt_stub: Tuple[int, int], skip: Set[str], preferred_xs: List[int]) -> List[Tuple[int, int]]:
        xs = {src_stub[0], tgt_stub[0], global_left - 60, global_left - 120, global_left - 180, global_right + 60}
        ys = set(row_corridors)
        ys.update({src_stub[1] - 60, src_stub[1] + 60, tgt_stub[1] - 60, tgt_stub[1] + 60})

        for x in preferred_xs:
            xs.add(int(x))

        for rid, rect in bounds.items():
            if rid in skip:
                continue
            ex, ey, ew, eh = _expand_rect_for_flow(rid, rect)
            xs.update([ex - 24, ex + ew + 24])
            ys.update([ey - 24, ey + eh + 24])

        xs = sorted(int(v) for v in xs)
        ys = sorted(int(v) for v in ys)

        points = []
        for x in xs:
            for y in ys:
                pt = (x, y)
                blocked = False
                for rid, rect in bounds.items():
                    if rid in skip:
                        continue
                    if _point_in_rect(pt, _expand_rect_for_flow(rid, rect)):
                        blocked = True
                        break
                if not blocked:
                    points.append(pt)

        #Add stubs explicitly
        if src_stub not in points:
            points.append(src_stub)
        if tgt_stub not in points:
            points.append(tgt_stub)

        by_y = defaultdict(list)
        by_x = defaultdict(list)
        for pt in points:
            by_y[pt[1]].append(pt)
            by_x[pt[0]].append(pt)
        for y in by_y:
            by_y[y].sort()
        for x in by_x:
            by_x[x].sort(key=lambda p: p[1])

        adj = defaultdict(list)
        for y, pts in by_y.items():
            for a, b in zip(pts, pts[1:]):
                if _segment_clear_flow(a, b, skip):
                    cost = abs(b[0] - a[0]) + _horizontal_penalty(y)
                    adj[a].append((b, 'H', cost))
                    adj[b].append((a, 'H', cost))
        for x, pts in by_x.items():
            for a, b in zip(pts, pts[1:]):
                if _segment_clear_flow(a, b, skip):
                    cost = abs(b[1] - a[1]) + _x_outer_penalty(x)
                    adj[a].append((b, 'V', cost))
                    adj[b].append((a, 'V', cost))

        start, goal = src_stub, tgt_stub
        pq = [(0, 0, start, None)]
        dist = {(start, None): 0}
        prev = {}
        end_state = None

        while pq:
            est, cost, node, dprev = heapq.heappop(pq)
            if cost != dist.get((node, dprev)):
                continue
            if node == goal:
                end_state = (node, dprev)
                break
            for nxt, dnow, edge_cost in adj[node]:
                turn_penalty = 28 if dprev and dprev != dnow else 0
                new_cost = cost + edge_cost + turn_penalty
                state = (nxt, dnow)
                if new_cost < dist.get(state, 10**18):
                    dist[state] = new_cost
                    prev[state] = (node, dprev)
                    heuristic = abs(goal[0] - nxt[0]) + abs(goal[1] - nxt[1])
                    heapq.heappush(pq, (new_cost + heuristic, new_cost, nxt, dnow))

        if end_state is None:
            #Fallback via nearest row corridor
            if row_corridors:
                ycorr = min(row_corridors, key=lambda y: abs(y - (src_stub[1] + tgt_stub[1]) // 2))
            else:
                ycorr = (src_stub[1] + tgt_stub[1]) // 2
            fallback = [src_stub, (src_stub[0], ycorr), (tgt_stub[0], ycorr), tgt_stub]
            return _compress_polyline(fallback)

        path = []
        cur = end_state
        while cur in prev:
            path.append(cur[0])
            cur = prev[cur]
        path.append(start)
        path.reverse()
        return _compress_polyline(path)

    flow_points_by_id: Dict[str, List[Tuple[int, int]]] = {}
    flow_edge_by_id: Dict[str, ET.Element] = {}
    flow_meta_by_id: Dict[str, Tuple[str, str, str, str]] = {}

    def add_flow_edge(flow_id: str, src: str, tgt: str, edge_id: str, label: str):
        if src not in node_bounds or tgt not in node_bounds:
            return

        ls = layer.get(src, 0)
        lt = layer.get(tgt, 0)
        forward = lt >= ls

        sx, sy, sw, sh = node_bounds[src]
        tx, ty, tw, th = node_bounds[tgt]

        src_x, src_y, src_side = _source_port(src, tgt)
        tgt_x, tgt_y, tgt_side = _target_port(tgt, src)

        src_stub = (src_x + 24, src_y) if src_side == 'R' else (src_x - 24, src_y)
        tgt_stub = (tgt_x - 24, tgt_y) if tgt_side == 'L' else (tgt_x + 24, tgt_y)

        preferred_xs = []
        if forward:
            corridor = forward_corridors.get((ls, ls + 1), (src_stub[0] + tgt_stub[0]) // 2)
            preferred_xs.extend([corridor, corridor - 30, corridor + 30])
        else:
            left_base = min(sx, tx) - 70
            preferred_xs.extend([left_base, left_base - 40, left_base - 80])

        middle = _route_flow_between_stubs(src_stub, tgt_stub, skip=set(), preferred_xs=preferred_xs)
        pts = [ (src_x, src_y), src_stub ]
        pts.extend(middle[1:-1])
        pts.extend([ tgt_stub, (tgt_x, tgt_y) ])
        pts = _compress_polyline(pts)

        edge = ET.SubElement(
            plane,
            _bpmndi('BPMNEdge'),
            {'id': edge_id, 'bpmnElement': flow_id},
        )

        for x, y in pts:
            ET.SubElement(edge, _di('waypoint'), {'x': str(int(x)), 'y': str(int(y))})

        flow_points_by_id[flow_id] = pts
        flow_edge_by_id[flow_id] = edge
        flow_meta_by_id[flow_id] = (src, tgt, label, src_side)

    for i, (flow_id, src, tgt, _lbl) in enumerate(sequence_flows):
        add_flow_edge(flow_id, src, tgt, f"BPMNEdge_Flow_{i}", _lbl)

    #Place labels after all sibling flow geometries
    for flow_id, pts in flow_points_by_id.items():
        src, tgt, label, src_side = flow_meta_by_id[flow_id]
        label_text = _flow_label_text(label)
        if not label_text:
            continue
        sibling_ids = [sid for sid in labeled_flow_ids_by_source.get(src, []) if sid != flow_id]
        branch_start_index = _flow_branch_start_index(flow_id, pts, flow_points_by_id, sibling_ids)
        rank_offset = label_rank_offset_for_flow.get(flow_id, 0)
        lx, ly, lw, lh = _best_flow_label_bounds(
            pts,
            label_text,
            rank_offset,
            src_bounds=node_bounds.get(src),
            tgt_bounds=node_bounds.get(tgt),
            src_kind=node_kind.get(src),
            src_side=src_side,
            branch_start_index=branch_start_index,
        )
        flow_label = ET.SubElement(flow_edge_by_id[flow_id], _bpmndi('BPMNLabel'))
        ET.SubElement(
            flow_label,
            _dc('Bounds'),
            {
                'x': str(int(lx)),
                'y': str(int(ly)),
                'width': str(int(lw)),
                'height': str(int(lh)),
            },
        )

    #Write XML file
    tree = ET.ElementTree(definitions)
    ET.indent(tree, space="  ", level=0)
    tree.write(out_path, encoding="utf-8", xml_declaration=True)


def _pick_paths_via_dialog() -> Tuple[str, str]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise RuntimeError("GUI file dialog is not available in this Python environment.") from exc

    root = tk.Tk()
    root.withdraw()
    root.update()

    input_path = filedialog.askopenfilename(
        title="Select CACAO JSON playbook",
        filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
    )
    if not input_path:
        root.destroy()
        raise SystemExit("No input file selected.")

    default_output = str(Path(input_path).with_suffix(".bpmn"))
    output_path = filedialog.asksaveasfilename(
        title="Save BPMN output as",
        defaultextension=".bpmn",
        initialfile=Path(default_output).name,
        initialdir=str(Path(default_output).parent),
        filetypes=[("BPMN files", "*.bpmn"), ("All files", "*.*")],
    )
    root.destroy()

    if not output_path:
        raise SystemExit("No output file selected.")

    return input_path, output_path


def _build_inline_bpmn_modeler_html(bpmn_xml: str, title: str) -> str:
    xml_js = json.dumps(bpmn_xml)
    title_js = json.dumps(title)
    template = '''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{page_title}</title>
  <link rel="stylesheet" href="https://unpkg.com/bpmn-js@18/dist/assets/diagram-js.css">
  <link rel="stylesheet" href="https://unpkg.com/bpmn-js@18/dist/assets/bpmn-js.css">
  <link rel="stylesheet" href="https://unpkg.com/bpmn-js@18/dist/assets/bpmn-font/css/bpmn-embedded.css">
  <style>
    html, body {{ height: 100%; margin: 0; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }}
    body {{ display: grid; grid-template-rows: auto 1fr; background: #f7f7f8; }}
    .toolbar {{
      display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
      padding: 10px 12px; background: #ffffff; border-bottom: 1px solid #ddd;
    }}
    .toolbar button {{
      border: 1px solid #ccc; background: #fff; border-radius: 8px; padding: 7px 10px; cursor: pointer;
      font-size: 14px;
    }}
    .toolbar button:hover {{ background: #f4f6fb; }}
    .toolbar .grow {{ flex: 1; }}
    .toolbar .title {{ font-weight: 600; color: #223; }}
    .toolbar .hint {{ color: #555; font-size: 13px; }}
    #canvas {{ width: 100%; height: 100%; background: #fcfcfd; }}
    .error {{ color: #b00020; font-size: 13px; margin-left: 8px; }}
  </style>
</head>
<body>
  <div class="toolbar">
    <div class="title" id="title"></div>
    <button id="fitBtn">Fit</button>
    <button id="zoomInBtn">Zoom +</button>
    <button id="zoomOutBtn">Zoom -</button>
    <button id="resetBtn">100%</button>
    <button id="handBtn">Hand tool</button>
    <button id="saveXmlBtn">Download BPMN</button>
    <button id="saveSvgBtn">Download SVG</button>
    <div class="grow"></div>
    <span class="hint">Drag, move and edit elements if needed.</span>
    <span id="error" class="error"></span>
  </div>
  <div id="canvas"></div>

  <script src="https://unpkg.com/bpmn-js@18/dist/bpmn-modeler.production.min.js"></script>
  <script>
    const initialXml = {xml_js};
    const titleText = {title_js};
    document.getElementById('title').textContent = titleText;

    const modeler = new BpmnJS({{
      container: '#canvas'
    }});

    const errorEl = document.getElementById('error');

    function download(filename, data, type) {{
      const blob = new Blob([data], {{ type }});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    }}

    async function openDiagram(xml) {{
      errorEl.textContent = '';
      try {{
        await modeler.importXML(xml);
        modeler.get('canvas').zoom('fit-viewport', 'auto');
      }} catch (err) {{
        console.error(err);
        errorEl.textContent = 'Could not open BPMN diagram.';
      }}
    }}

    document.getElementById('fitBtn').addEventListener('click', () => {{
      modeler.get('canvas').zoom('fit-viewport', 'auto');
    }});

    document.getElementById('zoomInBtn').addEventListener('click', () => {{
      modeler.get('zoomScroll').stepZoom(1);
    }});

    document.getElementById('zoomOutBtn').addEventListener('click', () => {{
      modeler.get('zoomScroll').stepZoom(-1);
    }});

    document.getElementById('resetBtn').addEventListener('click', () => {{
      modeler.get('canvas').zoom(1.0);
    }});

    document.getElementById('handBtn').addEventListener('click', () => {{
      modeler.get('handTool').activateMove();
    }});

    document.getElementById('saveXmlBtn').addEventListener('click', async () => {{
      try {{
        const {{ xml }} = await modeler.saveXML({{ format: true }});
        download('diagram.bpmn', xml, 'application/xml');
      }} catch (err) {{
        console.error(err);
        errorEl.textContent = 'Could not save BPMN XML.';
      }}
    }});

    document.getElementById('saveSvgBtn').addEventListener('click', async () => {{
      try {{
        const {{ svg }} = await modeler.saveSVG();
        download('diagram.svg', svg, 'image/svg+xml');
      }} catch (err) {{
        console.error(err);
        errorEl.textContent = 'Could not save SVG.';
      }}
    }});

    openDiagram(initialXml);
  </script>
</body>
</html>'''
    return template.format(page_title=title, xml_js=xml_js, title_js=title_js)


def _write_modeler_html_for_bpmn(bpmn_path: str, title: Optional[str] = None) -> str:
    bpmn_file = Path(bpmn_path).resolve()
    xml_text = bpmn_file.read_text(encoding='utf-8')
    modeler_path = bpmn_file.with_name(f"{bpmn_file.stem}_modeler.html")
    html_text = _build_inline_bpmn_modeler_html(xml_text, title or bpmn_file.name)
    modeler_path.write_text(html_text, encoding='utf-8')
    return str(modeler_path)


def _open_modeler_for_bpmn(bpmn_path: str, title: Optional[str] = None) -> str:
    import webbrowser
    modeler_path = _write_modeler_html_for_bpmn(bpmn_path, title=title)
    webbrowser.open(Path(modeler_path).resolve().as_uri())
    return modeler_path


def convert_cacao_to_bpmn_file(input_path: str, out_path: str) -> None:
    ir_pb = load_cacao_playbook(input_path)
    export_bpmn_process_xml(ir_pb, out_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Convert CACAO JSON to BPMN XML and optionally open a local BPMN modeler.")
    parser.add_argument("input", nargs="?", help="Path to a CACAO JSON file.")
    parser.add_argument("output", nargs="?", help="Output BPMN file path.")
    parser.add_argument("--gui", action="store_true", help="Open file dialogs for input and output paths.")
    parser.add_argument(
        "--no-open-modeler",
        action="store_true",
        help="Do not open the generated BPMN automatically in the browser modeler.",
    )
    args = parser.parse_args()

    if args.gui or (args.input is None and args.output is None):
        input_path, output_path = _pick_paths_via_dialog()
    else:
        if not args.input or not args.output:
            parser.error("Either provide both input and output paths, or use --gui.")
        input_path, output_path = args.input, args.output

    convert_cacao_to_bpmn_file(input_path, output_path)
    print(f"Wrote BPMN XML to: {output_path}")

    if not args.no_open_modeler:
        modeler_path = _open_modeler_for_bpmn(output_path, title=Path(output_path).name)
        print(f"Opened BPMN modeler: {modeler_path}")
