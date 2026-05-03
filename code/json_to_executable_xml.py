from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from json_to_xml import IRPlaybook, IRStep, load_cacao_playbook


#BPMN dependencies + Diagram Interchange Namespaces
BPMN_NS = "http://www.omg.org/spec/BPMN/20100524/MODEL"
BPMNDI_NS = "http://www.omg.org/spec/BPMN/20100524/DI"
DC_NS = "http://www.omg.org/spec/DD/20100524/DC"
DI_NS = "http://www.omg.org/spec/DD/20100524/DI"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
CAMUNDA_NS = "http://camunda.org/schema/1.0/bpmn"

ET.register_namespace("bpmn", BPMN_NS)
ET.register_namespace("bpmndi", BPMNDI_NS)
ET.register_namespace("dc", DC_NS)
ET.register_namespace("di", DI_NS)
ET.register_namespace("xsi", XSI_NS)
ET.register_namespace("camunda", CAMUNDA_NS)


def _bpmn(tag: str) -> str:
    return f"{{{BPMN_NS}}}{tag}"


def _bpmndi(tag: str) -> str:
    return f"{{{BPMNDI_NS}}}{tag}"


def _dc(tag: str) -> str:
    return f"{{{DC_NS}}}{tag}"


def _di(tag: str) -> str:
    return f"{{{DI_NS}}}{tag}"


#Configuration for the proof of concept
@dataclass
class ExecutablePocConfig:
    process_id: str = "ExecutablePocProcess"
    process_name: str = "Executable BPMN PoC"
    decision_step_name: str = "Evaluate Response"
    decision_var: str = "branch_decision"
    decision_default: bool = True
    task_mode: str = "task"  # "task" or "script"
    wait_task_names: Tuple[str, ...] = ()


@dataclass
class FlowRecord:
    flow_id: str
    source_ref: str
    target_ref: str
    name: Optional[str] = None
    condition_expression: Optional[str] = None


#Helper functions for validation
class UnsupportedExecutableSubsetError(ValueError):
    pass


def _step_name(step: IRStep) -> str:
    name = getattr(step, "name", None)
    if name:
        return str(name)
    return f"{getattr(step, 'step_type', 'step')}:{getattr(step, 'step_id', 'unknown')}"


def _get_step(ir: IRPlaybook, step_id: str) -> IRStep:
    step = ir.steps.get(step_id)
    if step is None:
        raise UnsupportedExecutableSubsetError(f"Referenced step '{step_id}' does not exist.")
    return step


def _as_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    return [str(value)]


def _single_target(value: object, field_name: str) -> str:
    values = _as_list(value)
    if len(values) != 1:
        raise UnsupportedExecutableSubsetError(
            f"Field '{field_name}' must contain exactly one target in the executable PoC, got: {values}"
        )
    return values[0]


def _groovy_single_quoted(text: object) -> str:
    s = str(text)
    s = s.replace("\\", "\\\\")
    s = s.replace("'", "\\'")
    s = s.replace("\n", "\\n")
    return f"'{s}'"


def _has_value(step: IRStep, attr: str) -> bool:
    value = getattr(step, attr, None)
    if value is None:
        return False
    if isinstance(value, list):
        return len(value) > 0
    return True


def _walk_parallel_linear_branch(ir: IRPlaybook, start_step_id: str) -> List[str]:
    """
    Restricted branch walker used only for the first executable PoC.
    A parallel branch may only contain a linear chain of action/single steps ending
    without additional branching.
    """
    visited: Set[str] = set()
    chain: List[str] = []
    current = start_step_id

    while current:
        if current in visited:
            raise UnsupportedExecutableSubsetError(
                f"Cycle detected inside parallel branch at step '{current}'."
            )
        visited.add(current)
        step = _get_step(ir, current)
        step_type = getattr(step, "step_type", None)

        if step_type == "end":
            break

        if step_type not in {"action", "single"}:
            raise UnsupportedExecutableSubsetError(
                "Executable parallel PoC currently supports only linear action chains "
                f"inside parallel branches, but found '{step_type}' at '{current}'."
            )

        if _has_value(step, "on_true") or _has_value(step, "on_false"):
            raise UnsupportedExecutableSubsetError(
                f"Conditional branching inside parallel branches is not supported in the PoC ('{current}')."
            )
        if _has_value(step, "on_success") or _has_value(step, "on_failure"):
            raise UnsupportedExecutableSubsetError(
                f"on_success/on_failure branching inside parallel branches is not supported in the PoC ('{current}')."
            )
        if _has_value(step, "next_steps"):
            raise UnsupportedExecutableSubsetError(
                f"Nested parallel branches are not supported in the PoC ('{current}')."
            )

        chain.append(current)
        next_step = getattr(step, "on_completion", None)
        if not next_step:
            break
        next_step_str = str(next_step)
        next_step_obj = _get_step(ir, next_step_str)
        if getattr(next_step_obj, "step_type", None) == "end":
            break
        current = next_step_str

    return chain


def validate_executable_subset(ir: IRPlaybook) -> None:
    supported_types = {"start", "end", "action", "single", "if-condition", "parallel"}

    if getattr(ir, "workflow_exception", None):
        raise UnsupportedExecutableSubsetError(
            "workflow_exception is intentionally not part of this first executable PoC."
        )

    for step_id, step in ir.steps.items():
        step_type = getattr(step, "step_type", None)

        if step_type not in supported_types:
            raise UnsupportedExecutableSubsetError(
                f"Step '{step_id}' uses unsupported step type '{step_type}' for the executable PoC."
            )

        if step_type in {"action", "single", "start", "end"}:
            if _has_value(step, "on_success") or _has_value(step, "on_failure"):
                raise UnsupportedExecutableSubsetError(
                    f"Step '{step_id}' uses on_success/on_failure, which is not yet supported in the executable PoC."
                )
            if _has_value(step, "cases") or _has_value(step, "default"):
                raise UnsupportedExecutableSubsetError(
                    f"Step '{step_id}' uses switch-case style transitions, which are not supported in the executable PoC."
                )

        if step_type == "if-condition":
            if not (_has_value(step, "on_true") and _has_value(step, "on_false")):
                raise UnsupportedExecutableSubsetError(
                    f"if-condition '{step_id}' must define both on_true and on_false in the executable PoC."
                )

            _single_target(getattr(step, "on_true", None), "on_true")
            _single_target(getattr(step, "on_false", None), "on_false")

            if _has_value(step, "on_success") or _has_value(step, "on_failure"):
                raise UnsupportedExecutableSubsetError(
                    f"if-condition '{step_id}' mixes incompatible branch semantics for the executable PoC."
                )
            if _has_value(step, "cases") or _has_value(step, "default"):
                raise UnsupportedExecutableSubsetError(
                    f"if-condition '{step_id}' uses switch-style branching, which is not supported in the executable PoC."
                )

        if step_type == "parallel":
            next_steps = _as_list(getattr(step, "next_steps", None))
            if len(next_steps) < 2:
                raise UnsupportedExecutableSubsetError(
                    f"parallel step '{step_id}' must define at least two next_steps in the executable PoC."
                )
            for branch_start in next_steps:
                _walk_parallel_linear_branch(ir, branch_start)


#BPMN Exporter
class ExecutableBpmnPocExporter:
    def __init__(self, ir: IRPlaybook, config: ExecutablePocConfig):
        self.ir = ir
        self.config = config

        self.definitions = ET.Element(
            _bpmn("definitions"),
            {
                "id": "Definitions_ExecutablePoc",
                "targetNamespace": "https://example.org/cacao/executable-poc",
            },
        )
        self.process = ET.SubElement(
            self.definitions,
            _bpmn("process"),
            {
                "id": self.config.process_id,
                "name": self.config.process_name,
                "isExecutable": "true",
                f"{{{CAMUNDA_NS}}}historyTimeToLive": "180",
            },
        )

        self.node_ids: Dict[str, str] = {}
        self.parallel_split_ids: Dict[str, str] = {}
        self.parallel_join_ids: Dict[str, str] = {}
        self.created_nodes: Set[str] = set()
        self.created_flows: Set[Tuple[str, str, str]] = set()
        self.flow_records: List[FlowRecord] = []
        self.flow_counter = 1
        self.compiled_steps: Set[str] = set()
        self.node_kind_by_id: Dict[str, str] = {}
        self.node_bounds_by_id: Dict[str, Tuple[float, float, float, float]] = {}

    #Helper functions for ids
    def _node_id_for_step(self, step_id: str) -> str:
        return f"Step_{step_id.replace('-', '_')}"

    def _parallel_split_id(self, step_id: str) -> str:
        return f"ParallelSplit_{step_id.replace('-', '_')}"

    def _parallel_join_id(self, step_id: str) -> str:
        return f"ParallelJoin_{step_id.replace('-', '_')}"

    def _new_flow_id(self) -> str:
        flow_id = f"Flow_{self.flow_counter}"
        self.flow_counter += 1
        return flow_id

    #Functions for node creation
    def _ensure_simple_node(self, step_id: str) -> str:
        if step_id in self.node_ids:
            return self.node_ids[step_id]

        step = _get_step(self.ir, step_id)
        step_type = getattr(step, "step_type", None)
        node_id = self._node_id_for_step(step_id)
        name = _step_name(step)

        if step_type == "start":
            ET.SubElement(self.process, _bpmn("startEvent"), {"id": node_id, "name": name})
            self.node_kind_by_id[node_id] = "startEvent"
        elif step_type == "end":
            ET.SubElement(self.process, _bpmn("endEvent"), {"id": node_id, "name": name})
            self.node_kind_by_id[node_id] = "endEvent"
        elif step_type in {"action", "single"}:
            created_kind = self._add_action_node(node_id=node_id, step=step)
            self.node_kind_by_id[node_id] = created_kind
        elif step_type == "if-condition":
            ET.SubElement(self.process, _bpmn("exclusiveGateway"), {"id": node_id, "name": name})
            self.node_kind_by_id[node_id] = "exclusiveGateway"
        else:
            raise UnsupportedExecutableSubsetError(
                f"_ensure_simple_node cannot create step type '{step_type}' for '{step_id}'."
            )

        self.node_ids[step_id] = node_id
        self.created_nodes.add(node_id)
        return node_id

    def _ensure_parallel_nodes(self, step_id: str) -> Tuple[str, str]:
        if step_id in self.parallel_split_ids and step_id in self.parallel_join_ids:
            return self.parallel_split_ids[step_id], self.parallel_join_ids[step_id]

        step = _get_step(self.ir, step_id)
        split_id = self._parallel_split_id(step_id)
        join_id = self._parallel_join_id(step_id)
        split_name = _step_name(step)
        join_name = f"Join {_step_name(step)}"

        ET.SubElement(self.process, _bpmn("parallelGateway"), {"id": split_id, "name": split_name})
        ET.SubElement(self.process, _bpmn("parallelGateway"), {"id": join_id, "name": join_name})

        self.parallel_split_ids[step_id] = split_id
        self.parallel_join_ids[step_id] = join_id
        self.created_nodes.add(split_id)
        self.created_nodes.add(join_id)
        self.node_kind_by_id[split_id] = "parallelGateway"
        self.node_kind_by_id[join_id] = "parallelGateway"
        return split_id, join_id
    
    def _is_wait_task(self, step: IRStep) -> bool:
        step_name = _step_name(step).strip().lower()
        configured = {name.strip().lower() for name in self.config.wait_task_names if name.strip()}
        return step_name in configured

    def _add_action_node(self, node_id: str, step: IRStep) -> str:
        task_name = _step_name(step)

        if self._is_wait_task(step):
            ET.SubElement(
                self.process,
                _bpmn("userTask"),
                {
                    "id": node_id,
                    "name": task_name,
                },
            )
            return "userTask"

        if self.config.task_mode == "task":
            ET.SubElement(
                self.process,
                _bpmn("task"),
                {
                    "id": node_id,
                    "name": task_name,
                },
            )
            return "task"

        task_el = ET.SubElement(
            self.process,
            _bpmn("scriptTask"),
            {
                "id": node_id,
                "name": task_name,
                "scriptFormat": "groovy",
            },
        )
        script_el = ET.SubElement(task_el, _bpmn("script"))
        script_el.text = self._build_script_body(step)
        return "scriptTask"

    def _build_script_body(self, step: IRStep) -> str:
        lines: List[str] = []
        step_name = _step_name(step)

        lines.append(f"println({_groovy_single_quoted(f'Executing step: {step_name}')})")

        commands = getattr(step, "commands", None) or []
        if isinstance(commands, list):
            for idx, cmd in enumerate(commands):
                lines.append(
                    f"println({_groovy_single_quoted(f'Command[{idx}]: {cmd}')})"
                )
        elif commands:
            lines.append(
                f"println({_groovy_single_quoted(f'Command: {commands}')})"
            )

        configured_name = self.config.decision_step_name.strip().lower()
        if configured_name and step_name.strip().lower() == configured_name:
            bool_text = "true" if self.config.decision_default else "false"
            lines.append(
                f'execution.setVariable("{self.config.decision_var}", {bool_text})'
            )
            lines.append(
                f"println({_groovy_single_quoted(f'Set decision variable {self.config.decision_var} = {bool_text}')})"
            )

        return "\n".join(lines)

    #Creation of sequence flow
    def _add_sequence_flow(
        self,
        source_ref: str,
        target_ref: str,
        *,
        name: Optional[str] = None,
        condition_expression: Optional[str] = None,
    ) -> None:
        key = (source_ref, target_ref, condition_expression or "")
        if key in self.created_flows:
            return
        self.created_flows.add(key)

        flow_id = self._new_flow_id()
        attrs = {
            "id": flow_id,
            "sourceRef": source_ref,
            "targetRef": target_ref,
        }
        if name:
            attrs["name"] = name

        flow_el = ET.SubElement(self.process, _bpmn("sequenceFlow"), attrs)
        if condition_expression:
            cond_el = ET.SubElement(
                flow_el,
                _bpmn("conditionExpression"),
                {f"{{{XSI_NS}}}type": "bpmn:tFormalExpression"},
            )
            cond_el.text = condition_expression

        self.flow_records.append(
            FlowRecord(
                flow_id=flow_id,
                source_ref=source_ref,
                target_ref=target_ref,
                name=name,
                condition_expression=condition_expression,
            )
        )

    #Compiling functions
    def compile(self) -> ET.Element:
        workflow_start_id = getattr(self.ir, "workflow_start", None)
        if not workflow_start_id:
            raise UnsupportedExecutableSubsetError("The playbook does not define workflow_start.")

        #Ensure that nodes exist
        for step_id, step in self.ir.steps.items():
            step_type = getattr(step, "step_type", None)
            if step_type == "parallel":
                self._ensure_parallel_nodes(step_id)
            else:
                self._ensure_simple_node(step_id)

        #Compile from start
        self._compile_step(str(workflow_start_id))
        self._build_bpmndi(str(workflow_start_id))
        return self.definitions

    def _compile_step(self, step_id: str) -> None:
        if step_id in self.compiled_steps:
            return
        self.compiled_steps.add(step_id)

        step = _get_step(self.ir, step_id)
        step_type = getattr(step, "step_type", None)

        if step_type in {"start", "action", "single"}:
            source_id = self._ensure_simple_node(step_id)
            next_step = getattr(step, "on_completion", None)
            if next_step:
                target_id = self._entry_node_id(str(next_step))
                self._add_sequence_flow(source_id, target_id)
                self._compile_step(str(next_step))
            return

        if step_type == "end":
            return

        if step_type == "if-condition":
            gateway_id = self._ensure_simple_node(step_id)
            on_true = _single_target(getattr(step, "on_true", None), "on_true")
            on_false = _single_target(getattr(step, "on_false", None), "on_false")

            true_target = self._entry_node_id(on_true)
            false_target = self._entry_node_id(on_false)

            decision_var = self.config.decision_var
            self._add_sequence_flow(
                gateway_id,
                true_target,
                name="True",
                condition_expression=f"${{{decision_var}}}",
            )
            self._add_sequence_flow(
                gateway_id,
                false_target,
                name="False",
                condition_expression=f"${{!{decision_var}}}",
            )

            self._compile_step(on_true)
            self._compile_step(on_false)
            return

        if step_type == "parallel":
            split_id, join_id = self._ensure_parallel_nodes(step_id)
            next_steps = _as_list(getattr(step, "next_steps", None))

            for branch_start in next_steps:
                branch_chain = _walk_parallel_linear_branch(self.ir, branch_start)
                if not branch_chain:
                    raise UnsupportedExecutableSubsetError(
                        f"Parallel branch '{branch_start}' of '{step_id}' is empty."
                    )

                branch_entry = self._ensure_simple_node(branch_chain[0])
                self._add_sequence_flow(split_id, branch_entry)

                for idx, chain_step_id in enumerate(branch_chain):
                    self._ensure_simple_node(chain_step_id)
                    if idx < len(branch_chain) - 1:
                        src = self._ensure_simple_node(chain_step_id)
                        tgt = self._ensure_simple_node(branch_chain[idx + 1])
                        self._add_sequence_flow(src, tgt)
                        self.compiled_steps.add(chain_step_id)
                    else:
                        terminal_id = self._ensure_simple_node(chain_step_id)
                        self._add_sequence_flow(terminal_id, join_id)
                        self.compiled_steps.add(chain_step_id)

            next_after_join = getattr(step, "on_completion", None)
            if next_after_join:
                target_id = self._entry_node_id(str(next_after_join))
                self._add_sequence_flow(join_id, target_id)
                self._compile_step(str(next_after_join))
            return

        raise UnsupportedExecutableSubsetError(
            f"Unsupported compilation step type '{step_type}' for '{step_id}'."
        )

    def _entry_node_id(self, step_id: str) -> str:
        step = _get_step(self.ir, step_id)
        if getattr(step, "step_type", None) == "parallel":
            split_id, _ = self._ensure_parallel_nodes(step_id)
            return split_id
        return self._ensure_simple_node(step_id)

    #BPMN Diagram Interchange generation
    def _build_bpmndi(self, workflow_start_step_id: str) -> None:
        self._assign_layout(workflow_start_step_id)

        diagram = ET.SubElement(
            self.definitions,
            _bpmndi("BPMNDiagram"),
            {"id": "BPMNDiagram_ExecutablePoc"},
        )
        plane = ET.SubElement(
            diagram,
            _bpmndi("BPMNPlane"),
            {
                "id": "BPMNPlane_ExecutablePoc",
                "bpmnElement": self.config.process_id,
            },
        )

        for node_id, (x, y, width, height) in sorted(
            self.node_bounds_by_id.items(), key=lambda item: (item[1][0], item[1][1], item[0])
        ):
            shape = ET.SubElement(
                plane,
                _bpmndi("BPMNShape"),
                {
                    "id": f"{node_id}_di",
                    "bpmnElement": node_id,
                },
            )
            ET.SubElement(
                shape,
                _dc("Bounds"),
                {
                    "x": self._fmt(x),
                    "y": self._fmt(y),
                    "width": self._fmt(width),
                    "height": self._fmt(height),
                },
            )

        for flow in self.flow_records:
            if flow.source_ref not in self.node_bounds_by_id or flow.target_ref not in self.node_bounds_by_id:
                continue
            edge = ET.SubElement(
                plane,
                _bpmndi("BPMNEdge"),
                {
                    "id": f"{flow.flow_id}_di",
                    "bpmnElement": flow.flow_id,
                },
            )
            for x, y in self._edge_waypoints(flow.source_ref, flow.target_ref):
                ET.SubElement(
                    edge,
                    _di("waypoint"),
                    {
                        "x": self._fmt(x),
                        "y": self._fmt(y),
                    },
                )

    def _assign_layout(self, workflow_start_step_id: str) -> None:
        start_node_id = self._entry_node_id(workflow_start_step_id)
        outgoing: Dict[str, List[str]] = {}
        incoming: Dict[str, List[str]] = {}

        for flow in self.flow_records:
            outgoing.setdefault(flow.source_ref, []).append(flow.target_ref)
            incoming.setdefault(flow.target_ref, []).append(flow.source_ref)
            outgoing.setdefault(flow.target_ref, [])
            incoming.setdefault(flow.source_ref, [])

        if start_node_id not in outgoing:
            outgoing[start_node_id] = []
        if start_node_id not in incoming:
            incoming[start_node_id] = []

        layers: Dict[str, int] = {start_node_id: 0}
        queue: List[str] = [start_node_id]
        while queue:
            current = queue.pop(0)
            current_layer = layers[current]
            for target in outgoing.get(current, []):
                proposed = current_layer + 1
                if target not in layers or proposed > layers[target]:
                    layers[target] = proposed
                    queue.append(target)

        max_layer = max(layers.values()) if layers else 0
        nodes_by_layer: Dict[int, List[str]] = {layer: [] for layer in range(max_layer + 1)}
        for node_id, layer in layers.items():
            nodes_by_layer.setdefault(layer, []).append(node_id)

        slot_values: Dict[str, float] = {start_node_id: 0.0}
        proposals: Dict[str, List[float]] = {}

        for layer in range(max_layer + 1):
            for node_id in nodes_by_layer.get(layer, []):
                current_slot = slot_values.get(node_id, 0.0)
                children = outgoing.get(node_id, [])
                if not children:
                    continue
                if len(children) == 1:
                    proposals.setdefault(children[0], []).append(current_slot)
                else:
                    offsets = self._centered_offsets(len(children), spacing=2.0)
                    for child, offset in zip(children, offsets):
                        proposals.setdefault(child, []).append(current_slot + offset)

            for node_id in nodes_by_layer.get(layer + 1, []):
                if node_id == start_node_id:
                    continue
                values = proposals.get(node_id, [])
                if values:
                    slot_values[node_id] = sum(values) / len(values)
                else:
                    slot_values.setdefault(node_id, 0.0)

        base_x = 120.0
        base_y = 240.0
        layer_dx = 180.0
        slot_dy = 90.0

        for node_id, layer in layers.items():
            width, height = self._node_size(node_id)
            x = base_x + layer * layer_dx
            y = base_y + slot_values.get(node_id, 0.0) * slot_dy
            self.node_bounds_by_id[node_id] = (x, y, width, height)

    def _node_size(self, node_id: str) -> Tuple[float, float]:
        kind = self.node_kind_by_id.get(node_id, "task")
        if kind in {"startEvent", "endEvent"}:
            return (36.0, 36.0)
        if kind in {"exclusiveGateway", "parallelGateway"}:
            return (50.0, 50.0)
        return (110.0, 80.0)

    def _centered_offsets(self, count: int, spacing: float) -> List[float]:
        if count == 1:
            return [0.0]
        center = (count - 1) / 2.0
        return [(idx - center) * spacing for idx in range(count)]

    def _edge_waypoints(self, source_ref: str, target_ref: str) -> List[Tuple[float, float]]:
        sx, sy, sw, sh = self.node_bounds_by_id[source_ref]
        tx, ty, tw, th = self.node_bounds_by_id[target_ref]

        start_x = sx + sw
        start_y = sy + sh / 2.0
        end_x = tx
        end_y = ty + th / 2.0

        if abs(start_y - end_y) < 1e-6:
            return [(start_x, start_y), (end_x, end_y)]

        mid_x = (start_x + end_x) / 2.0
        return [
            (start_x, start_y),
            (mid_x, start_y),
            (mid_x, end_y),
            (end_x, end_y),
        ]

    def _fmt(self, value: float) -> str:
        return f"{value:.1f}"


#Creation of BPMN file
def write_bpmn_xml(definitions: ET.Element, out_path: Path) -> None:
    tree = ET.ElementTree(definitions)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(out_path, encoding="utf-8", xml_declaration=True)


#Command Line Interface
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Restricted executable BPMN proof-of-concept exporter for CACAO playbooks."
    )
    parser.add_argument("input", help="Path to the CACAO JSON playbook.")
    parser.add_argument("output", help="Path to the generated executable BPMN XML file.")
    parser.add_argument(
        "--decision-step-name",
        default="Evaluate Response",
        help="Name of the action step that should set the Boolean decision variable for the XOR PoC.",
    )
    parser.add_argument(
        "--decision-var",
        default="branch_decision",
        help="Name of the Boolean process variable used for if-condition routing.",
    )
    parser.add_argument(
        "--decision-default",
        choices=["true", "false"],
        default="true",
        help="Default value assigned to the decision variable in the configured decision step.",
    )
    parser.add_argument(
        "--process-id",
        default="ExecutablePocProcess",
        help="BPMN process id for the generated executable model.",
    )
    parser.add_argument(
        "--process-name",
        default="Executable BPMN PoC",
        help="BPMN process name for the generated executable model.",
    )
    parser.add_argument(
        "--task-mode",
        choices=["task", "script"],
        default="task",
        help="How action/single steps should be exported in the executable PoC.",
    )
    parser.add_argument(
    "--wait-task-names",
    default="",
    help='Comma-separated task names that should be exported as userTask, e.g. "Be Happy,Be Angry".',
    )
    return parser

#Script entry point
def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_path = Path(args.output)

    try:
        ir = load_cacao_playbook(str(input_path))
        validate_executable_subset(ir)

        config = ExecutablePocConfig(
            process_id=args.process_id,
            process_name=args.process_name,
            decision_step_name=args.decision_step_name,
            decision_var=args.decision_var,
            decision_default=(args.decision_default.lower() == "true"),
            task_mode=args.task_mode,
            wait_task_names=tuple(
                part.strip() for part in args.wait_task_names.split(",") if part.strip()
            ),
        )

        exporter = ExecutableBpmnPocExporter(ir=ir, config=config)
        definitions = exporter.compile()
        write_bpmn_xml(definitions, output_path)

        print(f"Executable BPMN PoC written to: {output_path}")
        return 0
    except UnsupportedExecutableSubsetError as exc:
        print(f"Executable subset validation failed: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())