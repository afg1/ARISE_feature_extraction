COPY(
    select urs, secondary_structure, model_id, so_term_id from rnc_secondary_structure_layout rssl
    join rnc_secondary_structure_layout_models rssm on rssl.model_id = rssm.id
) TO STDOUT CSV
