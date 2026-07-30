# Dataflow Script Content Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Accept Qlik script text directly while preserving file-path compatibility.

**Architecture:** Extend the typed CLI contract with a second mutually exclusive source. Resolve and validate the source in the application layer, then expose only safe source metadata.

**Tech Stack:** Python 3.10+, argparse, dataclasses, hashlib, pytest, Ruff.

## Tasks

- [x] Add failing CLI tests for direct content, empty values, and source conflict.
- [x] Extend `ArgumentosDataflowScript` and argument detection.
- [x] Resolve file or direct content with a shared UTF-8 byte limit.
- [x] Remove raw script values from result contracts and add SHA-256 metadata.
- [x] Add process-level CLI test for multiline content.
- [x] Document Talend-safe argument passing and shell caveats.
- [x] Run full lint, formatting, tests, coverage, build, and clean-wheel verification.
