# Dynamo to CL via AWS CLI

Access Dynamo data via the AWS CLI

There are some existing solutions to get to Dynamo from CL but none of them worked with the security setup I needed.  
AWS-sign4 looks promising to directly consume the low level API, but wrapping the AWS CLI seems easier for small data volumes.  

# Sample usage

```lisp

;; drop code in ~/common-lisp/dyna-clc
(ql:quickload :dyna-clc)

(defparameter *table-data* (dclc:scan-table "SomeTable" "default"))

;; the CLI returns the data with the JSON format { "Attribute": { "Type": "Value" } }, so helpers are provided to filter
;; the output or get values listed directly.

;; return the items where "Attribute" matches "value" exactly
(dclc:filter-val *table-data* "Attribute" "value")

;; return the items where "Attribute" matches "value" as substring
(dclc:filter-val *table-data* "Attribute" "value" :contains t)

;; filter-vals chains calls to filter-val, if there is a "*" somewhere in the string it uses :contains t
(defparameter *single-item* (first (dclc:filter-vals *table-data* '(("Attribute" "val*") ("AnotherAttr" "777")))))

(setf (dclc:val *single-item* "Attribute") "a new value")

;; assuming "Attribute" is the PartitionKey, this will actually insert a new element
(dclc:upsert-item "SomeTable" *single-item* "default")

;; continuing from the sample above, delete the newly inserted element
(dclc::delete-by-key "SomeTable" "Attribute" "a new value" "default")
```

The idea is to use this as a building block to create more specialized functions for the particular data in your tables, or for quick queries in the REPL.

