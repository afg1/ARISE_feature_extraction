process download_fasta {

    input:
        val(_flag)

    output:
        path("*.fasta")

    """
    wget https://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/sequences/rnacentral_active.fasta.gz
    gzip -d rnacentral_active.fasta.gz

    """
}

process get_structures {

    input:
        path(query)
    
    output:
        path("r2dt_hits.csv")

    """
    psql -f $query $PGDATABASE > r2dt_hits.csv
    """
}

process extract_features {
    container 'oras://ghcr.io/afg1/arise:vrna'
    input:
        tuple path(fasta), path(structures)

    output:
        path("secondary_structure_energy_and_model.csv")

    """
    mfe.py $fasta $structures secondary_structure_energy_and_model.csv
    """

}


workflow {
    emit: 
        features

    Channel.of("ready") | set { _ready }
    Channel.fromPath("structure_query.sql") | set { struc_query }

    struc_query | get_structures \
    | set { structures }
    
    _ready | download_fasta \
    | splitFasta(by: 10000, file: true) \
    | set { sequences }

    sequences.combine(structures) | extract_features \
    | collectFile() { csvfile -> [csvfile.name, csvfile.text] } \
    | set { features }

}