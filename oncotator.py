#!/usr/bin/env python2.7

import itertools
import requests
import sys
import types

if len(sys.argv) < 3:
    print """Usage: oncotator.py INPUT_FILE OUTPUT_FILE"""
    sys.exit(1)

form_url = 'http://www.broadinstitute.org/oncotator/'
api_url = 'http://www.broadinstitute.org/oncotator/mutation/%s_%s_%s_%s_%s'
chunk_size_lines = 7500

input_file = sys.argv[1]
output_file = sys.argv[2]

# From http://docs.python.org/2/library/itertools.html#recipes
def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

is_first_chunk = True
chunk_index = 1

def jsonLookup(key, json):
    if key is None:
        return None
    elif isinstance(key, basestring):
        return key
    elif isinstance(key, types.FunctionType):
        key(json)
    elif isinstance(key, list):
        if key:
            if isinstance(json, dict):
                if key[0] in json:
                    return jsonLookup(key[1:], json[key[0]])
                else:
                    return None
            elif isinstance(json, list):
                for element in json:
                    result = jsonLookup(key, element)
                    if result is not None:
                        return result
                return None
            else:
                return json
        else:
            return json
    else:
        return None

def otherTranscripts(json):
    def summarizeTranscript(t):
        return '%s_%s' % (t['transcript_id'], t['variant_classification'])
    transcripts = [summarizeTranscript(t) for t in json['transcripts'][1:]]
    return '|'.join(transcripts)

def transcriptPosition(json):
    start = jsonLookup(['transcripts', 'absolute_transcript_position_start'], json)
    end = jsonLookup(['transcripts', 'absolute_transcript_position_end'], json)
    if start and end:
        if start == end:
            return start
        else:
            return '%s_%s' % (start, end)
    else:
        return None

def transcriptLookup(key, json):
    if 'best_canonical_transcript' in json:
        transcript_index = int(json['best_canonical_transcript'])
        if key in json['transcripts'][transcript_index]:
            return json['transcripts'][transcript_index][key]
    return jsonLookup(['transcripts', key], json)

column_locs = [
    ('Hugo_Symbol', lambda json: transcriptLookup('gene', json)),
    ('Entrez_Gene_Id', None),
    ('Center', 'broad.mit.edu'),
    ('NCBI_Build', '37'),
    ('Chromosome', ['chr']),
    ('Start_position', ['start']),
    ('End_position', ['end']),
    ('Strand', lambda json: transcriptLookup('strand', json)),
    ('Variant_Classification', lambda json: transcriptLookup('variant_classification', json)),
    ('Variant_Type', ['variant_type']),
    ('Reference_Allele', ['reference_allele']),
    ('Tumor_Seq_Allele1', ['observed_allele']),
    ('Tumor_Seq_Allele2', ['observed_allele']),
    ('dbSNP_RS', ['dbSNP_RS']),
    ('dbSNP_Val_Status', ['dbSNP_Val_Status']),
    ('Tumor_Sample_Barcode', None),
    ('Matched_Norm_Sample_Barcode', None),
    ('Match_Norm_Seq_Allele1', None),
    ('Match_Norm_Seq_Allele2', None),
    ('Tumor_Validation_Allele1', None),
    ('Tumor_Validation_Allele2', None),
    ('Match_Norm_Validation_Allele1', None),
    ('Match_Norm_Validation_Allele2', None),
    ('Verification_Status', None),
    ('Validation_Status', None),
    ('Mutation_Status', None),
    ('Sequencing_Phase', None),
    ('Sequence_Source', None),
    ('Validation_Method', None),
    ('Score', None),
    ('BAM_file', None),
    ('Sequencer', None),
    ('Genome_Change', ['genome_change']),
    ('Annotation_Transcript', lambda json: transcriptLookup('transcript_id', json)),
    ('Transcript_Strand', lambda json: transcriptLookup('strand', json)),
    ('Transcript_Exon', lambda json: transcriptLookup('exon_affected', json)),
    ('Transcript_Position', transcriptPosition),
    ('cDNA_Change', lambda json: transcriptLookup('transcript_change', json)),
    ('Codon_Change', lambda json: transcriptLookup('codon_change', json)),
    ('Protein_Change', lambda json: transcriptLookup('protein_change', json)),
    ('Other_Transcripts', otherTranscripts),
    ('Refseq_mRNA_Id', None),   # TODO
    ('Refseq_prot_Id', None),   # TODO
    ('SwissProt_acc_Id', None), # TODO
    ('SwissProt_entry_Id', None), # TODO
    ('Description', lambda json: transcriptLookup('description', json)),
    ('UniProt_AApos', None),    # TODO
    ('UniProt_Region', None),# TODO
    ('UniProt_Site', None),# TODO
    ('UniProt_Natural_Variations', None),# TODO
    ('UniProt_Experimental_Info', None),# TODO
    ('GO_Biological_Process', None),# TODO
    ('GO_Cellular_Component', None),# TODO
    ('GO_Molecular_Function', None),# TODO
    ('COSMIC_overlapping_mutations', None),# TODO
    ('COSMIC_fusion_genes', None),# TODO
    ('COSMIC_tissue_types_affected', None),# TODO
    ('COSMIC_total_alterations_in_gene', None),
    ('Tumorscape_Amplification_Peaks', None),
    ('Tumorscape_Deletion_Peaks', None),
    ('TCGAscape_Amplification_Peaks', None),
    ('TCGAscape_Deletion_Peaks', None),
    ('DrugBank', None),# TODO
    ('PPH2_Class', None),# TODO
    ('PPH2_Prob', None),
    ('PPH2_FDR', None),
    ('PPH2_MSA_dScore', None),
    ('PPH2_MSA_Score1', None),
    ('PPH2_MSA_Score2', None),
    ('PPH2_MSA_Nobs', None),
    ('CCLE_ONCOMAP_overlapping_mutations', None),
    ('CCLE_ONCOMAP_total_mutations_in_gene', None),
    ('CGC_Mutation_Type', None),
    ('CGC_Translocation_Partner', None),
    ('CGC_Tumor_Types_Somatic', None),# TODO
    ('CGC_Tumor_Types_Germline', None),
    ('CGC_Other_Diseases', None),
    ('DNARepairGenes_Role', None),
    ('FamilialCancerDatabase_Syndromes', None),
    ('MUTSIG_Published_Results', None),
]

with open(input_file, 'r') as f, open(output_file, 'w') as out:
    for line in f:
        sys.stdout.write('.')
        sys.stdout.flush()

        # Submit the current line
        api_url_for_line = api_url % tuple(line.split('\t'))
        r = requests.get(api_url_for_line)

        # Format the resulting output
        json = r.json()
        cols = [jsonLookup(json_position, json) for name, json_position in column_locs]
        download_url = unicode.replace(r.url, '/report/', '/download/')
        output_chunk = requests.get(download_url).text
        sys.stdout.write('.')
        sys.stdout.flush()

        # Strip the first two header lines for all but the first chunk
        if is_first_chunk:
            is_first_chunk = False
        else:
            output_chunk = output_chunk.split('\n', 2)[2]

        # Append the result to the output file
        output_chunk_bytes = output_chunk.encode('UTF-8')
        out.write(output_chunk_bytes)
        sys.stdout.write('done. Fetched %s; wrote %d lines, %d bytes.\n' % (
            download_url, unicode.count(output_chunk, '\n'), len(output_chunk_bytes)))
        sys.stdout.flush()
