"""Reconstruct and store the final, cleaned datasets."""

from project_workflow.data_preparation import assign_recipient_iso_codes
from project_workflow.data_preparation import build_analysis_output_tables
from project_workflow.data_preparation import build_final_analysis_dataset
from project_workflow.data_preparation import build_per_capita_dataset
from project_workflow.data_preparation import build_recipient_year_panel
from project_workflow.data_preparation import load_input_data
from project_workflow.data_preparation import prepare_crdf_dataset
from project_workflow.data_preparation import prepare_mvi_dataset
from project_workflow.data_preparation import prepare_population_dataset
from project_workflow.data_preparation import prepare_sids_dataset
from project_workflow.data_preparation import save_final_outputs


# =======================================================================================
# V. PIPELINE ENTRYPOINT
# =======================================================================================

def main():
    """Run the full project workflow from raw inputs to final CSV outputs."""
    raw_data = load_input_data()
    project_recode_lookup = raw_data["projects_to_recode"]

    project_level_data = prepare_crdf_dataset(raw_data["CRDF"], project_recode_lookup)
    vulnerability_data = prepare_mvi_dataset(
        raw_data["MVI_vulnerability"],
        raw_data["MVI_resilience"],
    )

    project_level_data = assign_recipient_iso_codes(project_level_data)
    recipient_year_panel = build_recipient_year_panel(project_level_data)

    population_data = prepare_population_dataset(raw_data["population"])
    sids_reference_data = prepare_sids_dataset(raw_data["SIDS"])
    per_capita_data = build_per_capita_dataset(
        recipient_year_panel,
        population_data,
        vulnerability_data,
        sids_reference_data,
    )

    final_project_data = build_final_analysis_dataset(
        project_level_data,
        vulnerability_data,
    )
    analysis_tables = build_analysis_output_tables(final_project_data, per_capita_data)
    save_final_outputs(
        final_project_data,
        per_capita_data,
        analysis_tables = analysis_tables,
    )


if __name__ == '__main__':
    main()
