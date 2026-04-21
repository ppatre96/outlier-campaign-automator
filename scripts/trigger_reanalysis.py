#!/usr/bin/env python
"""CLI entry point for manual reanalysis triggering."""

import argparse
import logging
from src.reanalysis_loop import ReanalysisOrchestrator

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Trigger cohort reanalysis')
    parser.add_argument('--cohort-exclude', type=str, help='Cohort to exclude')
    parser.add_argument('--cohort-focus', type=str, help='Cohort to focus on')
    parser.add_argument('--reason', type=str, default='manual', help='Reason for reanalysis')
    
    args = parser.parse_args()
    
    orchestrator = ReanalysisOrchestrator()
    new_cohorts = orchestrator.trigger_reanalysis(
        cohort_to_exclude=args.cohort_exclude,
        cohort_to_focus=args.cohort_focus,
        reason=args.reason
    )
    
    log.info("Reanalysis complete: %d new cohorts", len(new_cohorts))
    return 0


if __name__ == '__main__':
    exit(main())
