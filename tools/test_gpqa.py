"""
GPQA Diamond — DeepSeek-V3.2 on EOS interactive (1 node × 8 H100)
=================================================================
Each arm is an independent eval job that starts its own SGLang server
via a heterogeneous Slurm job (no persistent server needed).

EOS has 8 GPUs per node (H100 80GB), so we use 1 node with EP=8.
Partition: interactive (2h max, < 5 min wait).

Usage:
  conda activate hle-dev
  python experiments_v5/run_gpqa_deepseek_v32_eos.py --arms 12,13
"""

import argparse

from nemo_skills.pipeline.cli import eval, wrap_arguments

from run_id import next_run_id

# ── Model / cluster ──────────────────────────────────────────────────────

MODEL = "/hf_models/DeepSeek-V3.2"
BENCHMARKS = "gpqa:5"
CLUSTER = "eos_interactive"
SERVER_GPUS = 8
SERVER_NODES = 1

CUSTOM_SANDBOX = (
    "/lustre/fsw/llmservice_nemo_reasoning/htamoyan/images/"
    "nemo-skills-sandbox-hle-sci-v8.sqsh"
)

BARE_PACKS = "physics_core,chemistry,quantum,astronomy"
DOC_PACKS = "physics_core_doc,chemistry_doc,quantum_doc,astronomy_doc"
KDENSE_PACKS = "physics_core_kdense,chemistry_kdense,quantum_kdense,astronomy_kdense,biology_kdense"
KDENSE_BROAD_PACKS = (
    "physics_core_kdense,chemistry_kdense,quantum_kdense,astronomy_kdense,"
    "biology_kdense,materials_kdense,quantum_computing_kdense,statistics_kdense"
)

SERVER_ARGS = (
    "--enable-dp-attention "
    "--ep-size 8 "
    "--dp 8 "
    "--tool-call-parser deepseekv32 "
    "--reasoning-parser deepseek-v3 "
    "--log-requests "
    "--mem-fraction-static=0.8 "
    """--model-loader-extra-config '{"enable_multithread_load":true,"num_threads":112}' """
)

INFERENCE_ARGS = (
    "++inference.temperature=1.0 "
    "++inference.top_p=0.95 "
    "++inference.tokens_to_generate=100000 "
)

SCIPYTHON_BASE = (
    '++tool_modules=["nemo_skills.mcp.servers.sci_python_tool::SciPythonTool"] '
    "++chat_template_kwargs.thinking=true "
)

SCIPYTHON_SEARCH = (
    '++tool_modules=["nemo_skills.mcp.servers.sci_python_tool::SciPythonTool",'
    '"nemo_skills.mcp.servers.arxiv_tool::ArxivSearchTool",'
    '"nemo_skills.mcp.servers.wikipedia_tool::WikipediaSearchTool"] '
    "++chat_template_kwargs.thinking=true "
)

SCIPYTHON_CHEM = (
    '++tool_modules=["nemo_skills.mcp.servers.sci_python_tool::SciPythonTool",'
    '"nemo_skills.mcp.servers.chem_mcp_tool::ChemMCPTool"] '
    "++chat_template_kwargs.thinking=true "
)

SCIPYTHON_ALL = (
    '++tool_modules=["nemo_skills.mcp.servers.sci_python_tool::SciPythonTool",'
    '"nemo_skills.mcp.servers.arxiv_tool::ArxivSearchTool",'
    '"nemo_skills.mcp.servers.wikipedia_tool::WikipediaSearchTool",'
    '"nemo_skills.mcp.servers.chem_mcp_tool::ChemMCPTool"] '
    "++chat_template_kwargs.thinking=true "
)

# ── Experiment arms ──────────────────────────────────────────────────────

# Each arm has a matching prompt_config with a system message describing available tools.
# This tells the model what's available so it doesn't re-import or ignore pre-loaded libs.
ARMS = [
    {   # Arm 1: BASELINE — no code, no tools, pure reasoning with thinking
        "key": "no-tool",
        "desc": "Baseline: pure reasoning with thinking, no tools",
        "extra_args": "++chat_template_kwargs.thinking=true ",
        "prompt_config": "eval/aai/mcq-4choices",
        "sandbox": False,
        "sandbox_container": None,
    },
    {   # Arm 2: + generic Python code execution (PythonTool)
        "key": "python",
        "desc": "+ PythonTool (generic code exec, default sandbox)",
        "extra_args": (
            '++tool_modules=["nemo_skills.mcp.servers.python_tool::PythonTool"] '
            "++chat_template_kwargs.thinking=true "
        ),
        "prompt_config": "eval/aai/mcq-4choices-python",
        "sandbox": True,
        "sandbox_container": None,
    },
    {   # Arm 3: + SciPythonTool with bare packs, default sandbox
        "key": "scipython-packs",
        "desc": "+ SciPythonTool bare packs (pre-imported libs, default sandbox)",
        "extra_args": (
            SCIPYTHON_BASE
            + f"++tool_overrides.SciPythonTool.packs=[{BARE_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython",
        "sandbox": True,
        "sandbox_container": None,
    },
    {   # Arm 4: + custom sandbox with rdkit, matplotlib, networkx
        "key": "scipython-packs-sandbox",
        "desc": "+ custom sandbox (rdkit/matplotlib/networkx installed)",
        "extra_args": (
            SCIPYTHON_BASE
            + f"++tool_overrides.SciPythonTool.packs=[{BARE_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython-sandbox",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
    },
    {   # Arm 5: + documented packs with usage examples in preamble
        "key": "scipython-packs-sandbox-doc",
        "desc": "+ documented packs (usage examples in preamble)",
        "extra_args": (
            SCIPYTHON_BASE
            + f"++tool_overrides.SciPythonTool.packs=[{DOC_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython-sandbox",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
    },
    {   # Arm 6: + ArXiv and Wikipedia search tools
        "key": "scipython-full-search",
        "desc": "+ ArXiv + Wikipedia search (retrieval-augmented)",
        "extra_args": (
            SCIPYTHON_SEARCH
            + f"++tool_overrides.SciPythonTool.packs=[{DOC_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython-search",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
        "installation_command": "pip install arxiv wikipedia",
    },
    {   # Arm 7: + ChemMCP chemistry toolkit (30+ chem tools)
        "key": "scipython-full-chem",
        "desc": "+ ChemMCP (30+ chemistry tools: SMILES, properties, reactions)",
        "extra_args": (
            SCIPYTHON_CHEM
            + f"++tool_overrides.SciPythonTool.packs=[{DOC_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython-chem",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
        "installation_command": "pip install chemmcp",
    },
    {   # Arm 8: + all external tools combined (search + chemistry)
        "key": "scipython-full-all",
        "desc": "+ all tools (ArXiv + Wikipedia + ChemMCP)",
        "extra_args": (
            SCIPYTHON_ALL
            + f"++tool_overrides.SciPythonTool.packs=[{DOC_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython-all",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
        "installation_command": "pip install arxiv wikipedia chemmcp",
    },
    {   # Arm 9: K-Dense packs (richer examples, RDKit in preamble, +biology)
        "key": "scipython-kdense",
        "desc": "+ K-Dense packs (richer examples, RDKit in preamble, +biology)",
        "extra_args": (
            SCIPYTHON_BASE
            + f"++tool_overrides.SciPythonTool.packs=[{KDENSE_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-kdense",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
    },
    {   # Arm 10: K-Dense broad (+materials, +quantum computing, +statistics)
        "key": "scipython-kdense-broad",
        "desc": "+ K-Dense broad (all domains: +materials, +quantum computing, +statistics)",
        "extra_args": (
            SCIPYTHON_BASE
            + f"++tool_overrides.SciPythonTool.packs=[{KDENSE_BROAD_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-kdense-broad",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
    },
    {   # Arm 11: System prompt with concrete usage examples for each library
        # Shows exact function calls and return values (e.g. Descriptors.MolWt(mol) → 46.07)
        # Hypothesis: concrete examples guide the model to use tools correctly
        # and avoid the "think less, code more" trap by showing how quick verification looks
        "key": "scipython-examples",
        "desc": "+ concrete usage examples in system prompt (inline API reference)",
        "extra_args": (
            SCIPYTHON_BASE
            + f"++tool_overrides.SciPythonTool.packs=[{KDENSE_PACKS}] "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython-examples",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
    },
    {   # Arm 12: NO preamble pre-loading — just a guide of what's available
        # Uses plain PythonTool (not SciPythonTool) so nothing is pre-imported.
        # System prompt lists available libraries by domain but model imports itself.
        # Hypothesis: avoids thinking displacement from preamble awareness,
        # model reasons deeply first, imports only what it actually needs.
        "key": "python-guide",
        "desc": "PythonTool + library guide (no pre-loading, model imports as needed)",
        "extra_args": (
            '++tool_modules=["nemo_skills.mcp.servers.python_tool::PythonTool"] '
            "++chat_template_kwargs.thinking=true "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython-guide",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
    },
    {   # Arm 13: PythonTool + K-Dense SKILL.md references on disk
        # No pre-loading. Model can read SKILL.md files for API reference.
        # System prompt includes inline examples for sympy + rdkit.
        # 178 library reference files available at /nemo_run/code/nemo_skills/references/
        # Uses keep_mounts_for_sandbox=True so sandbox can access /nemo_run/code/
        # Hypothesis: model has best-of-both — deep reasoning + on-demand API docs
        "key": "python-skills",
        "desc": "PythonTool + K-Dense SKILL.md references + inline examples (50K output)",
        "extra_args": (
            '++tool_modules=["nemo_skills.mcp.servers.python_tool::PythonTool"] '
            "++chat_template_kwargs.thinking=true "
            "++tool_overrides.PythonTool.max_output_characters=50000 "
        ),
        "prompt_config": "eval/aai/mcq-4choices-scipython-skills",
        "sandbox": True,
        "sandbox_container": CUSTOM_SANDBOX,
        "keep_mounts_for_sandbox": True,
    },
]

# ── Helpers ──────────────────────────────────────────────────────────────


def _expname(key, run_id):
    suffix = f"-{key}" if key else ""
    return f"hle_test_gpqa-dsv32{suffix}-r{run_id}"


def _output_dir(key, run_id):
    suffix = f"-{key}" if key else ""
    return f"/workspace/hle-experiments/test-gpqa-dsv32{suffix}-r{run_id}"


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arms", default="1,2,3,4,5,6,7,8,9,10,11,12,13",
                    help="Comma-separated arm indices to run (default: 1,2,3,4,5,6,7,8,9,10,11,12,13)")
    args = ap.parse_args()

    run_id = next_run_id("gpqa-dsv32")
    arm_indices = [int(x) for x in args.arms.split(",")]

    results = []
    for i in arm_indices:
        arm = ARMS[i - 1]
        name = _expname(arm["key"], run_id)
        odir = _output_dir(arm["key"], run_id)

        print(f"\n{'=' * 60}")
        print(f"  Arm {i}: {name}")
        print(f"  {arm['desc']}")
        print(f"  cluster: {CLUSTER}  gpus/node: {SERVER_GPUS}  nodes: {SERVER_NODES}  ep: 8")
        print(f"{'=' * 60}")

        prompt_override = f"++prompt_config={arm['prompt_config']} " if arm.get("prompt_config") else ""
        eval_kwargs = dict(
            ctx=wrap_arguments(INFERENCE_ARGS + arm["extra_args"] + prompt_override),
            cluster=CLUSTER,
            expname=name,
            model=MODEL,
            server_type="sglang",
            server_gpus=SERVER_GPUS,
            server_nodes=SERVER_NODES,
            server_args=SERVER_ARGS,
            benchmarks=BENCHMARKS,
            output_dir=odir,
            with_sandbox=arm["sandbox"],
            num_jobs=1,
            log_samples=True,
            wandb_project="hle",
            wandb_name=name,
            wandb_group="gpqa-deepseek-v32",
        )
        if arm.get("sandbox_container"):
            eval_kwargs["sandbox_container"] = arm["sandbox_container"]
        if arm.get("installation_command"):
            eval_kwargs["installation_command"] = arm["installation_command"]
        if arm.get("keep_mounts_for_sandbox"):
            eval_kwargs["keep_mounts_for_sandbox"] = True

        eval(**eval_kwargs)
        results.append((name, odir))

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"Submitted {len(results)} independent eval job(s) on {CLUSTER}")
    print(f"{'=' * 60}\n")
    for name, odir in results:
        print(f"  {name}")
        print(f"    ns summarize_results --cluster {CLUSTER} {odir}")
    print()


if __name__ == "__main__":
    main()
