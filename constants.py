# string constant
STATEMENT = "statement"
WHERE = "where"
SELECT = "select"
UPDATE = "update"
DELETE = "delete"
INSERT = "insert"
BEGIN = "begin"
COMMIT = "commit"

# cutoff constant

"""
If a column is updated more than UPDATE_CUTOFF.
a candidate index is no longer considered if 
    * a composite index (multi-column index) is referenced less than COMPOSITE_REFERENCE_CUTOFF_HIGH
    * a simple index (single-column index) is referenced less than SIMPLE_REFERENCE_CUT_OFF_HIGH
"""
UPDATE_CUTOFF = 0.05
COMPOSITE_REFERENCE_CUTOFF_HIGH = 0.2
SIMPLE_REFERENCE_CUT_OFF_HIGH = 0.2

"""
If a column (or columns) is used more than REFERENCE_CUTOFF_LOW 
it will be considered as a candidate index.
"""
REFERENCE_CUTOFF_LOW = 0.1

