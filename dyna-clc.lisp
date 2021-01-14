;;;; dyna-clc.lisp

(in-package #:dyna-clc)

(defun dynamo-operation (operation table-name parameters-list profile)
  "Run OPERATION on TABLE-NAME. PARAMETERS-LIST is a list of parameters to include in the call,
for example \"--query file://params.json\". PROFILE should be a profile name from your AWS
credentials file, if you use a single account most likely it should be \"default\"."
  (let ((cli-call `("aws"
                    "dynamodb"
                    ,operation
		    "--table-name"
		    ,table-name
                    ,@parameters-list
                    "--profile"
                    ,profile)))
    (uiop:run-program cli-call :output :string)))

(defun scan-table (table-name profile)
  "Returns all the items in TABLE-NAME. This operation can take a long time on even moderately-sized
tables.
See the DYNAMO-OPERATION docs for notes on the PROFILE parameter."
  (let ((raw-output (dynamo-operation "scan"
				      table-name
				      nil
				      profile)))
    (parse-items-list raw-output)))

(defun elements-by-key (table-name key-name key-value profile)
  "Returns all the items in TABLE-NAME whose KEY-NAME matches KEY-VALUE. In most cases this will
return a list with a single item, but if TABLE-NAME has a SortKey, then you'll get back more
elements.
See the DYNAMO-OPERATION docs for notes on the PROFILE parameter."
  (let* ((params (list "--key-condition-expression"
                       (format nil "~a = :key" key-name)
                       "--expression-attribute-values"
                       (format nil "{\":key\":{\"S\": \"~a\"}}" key-value)))
         (raw-output (dynamo-operation "query"
				      table-name
				      params
				      profile)))
    (parse-items-list raw-output)))

(defun delete-by-key (table-name key-name key-value profile)
  "Delete the item in TABLE-NAME where KEY-NAME matches KEY-VALUE. This function works only for
tables without SortKey, if you need to specify a secondary value for the delete, use DELETE-BY-KEYS
instead.
See the DYNAMO-OPERATION docs for notes on the PROFILE parameter."
  (dynamo-operation "delete-item"
                    table-name
                    (list "--key"
                          (format nil "{\"~a\":{\"S\": \"~a\"}}"
                                  key-name
                                  key-value))
                    profile))

(defun delete-by-keys (table-name key-type-value-alist profile)
  "Delete the item in TABLE-NAME that matches specified KEY-TYPE-VALUE-ALIST with the format
(keyName (type. keyValue). This function works for tables with a SortKey, if there is a single
key then DELETE-BY-KEY is easier to use.
See the DYNAMO-OPERATION docs for notes on the PROFILE parameter."
  (dynamo-operation "delete-item"
		    table-name
		    (list "--key"
			  (jonathan:to-json key-type-value-alist :from :alist))
                    profile))

(defun upsert-item (table-name item-data profile)
  "Upsert an item in TABLE-NAME using ITEM-DATA. If the PartitionKey (and optionally SortKey)
in ITEM-DATA matches an existing element, it will be updated, else you get a new element.
See the DYNAMO-OPERATION docs for notes on the PROFILE parameter."
  (let ((jonathan:*null-value* :null)
        (jonathan:*false-value* :false)
        (jonathan:*empty-array-value* :[]))
    (uiop:with-temporary-file (:stream tmp-stream :pathname tmp-pathname)
      (write-string (jonathan:to-json item-data :from :alist) tmp-stream)
      :close-stream
      (dynamo-operation "put-item"
			table-name
			(list "--item"
			      (concatenate 'string
					   "file://"
					   (uiop:native-namestring tmp-pathname)))
			profile))))

(defun parse-items-list (aws-json)
  "Use JONATHAN:PARSE to convert the JSON-representation of Dynamo items returned by the AWS CLI to
a bunch of alists, which are a bit more readable in the REPL."
  (let ((jonathan:*null-value* :null)
        (jonathan:*false-value* :false))
    (alexandria:assoc-value
     (jonathan:parse aws-json :as :alist)
     "Items"
     :test #'equal)))

(defun (setf val) (new-value data key)
  "Return the value of KEY from DATA as provided by dyna-clc.
A second return value indicates the AWS type definition, for example \"1234\" will be
a string literal if this value is \"S\", or a number if it's \"N\"."
  (let ((found (second (assoc key data :test #'equal))))
    (when found
      (values (setf (cdr found) new-value)
	      (car found)))))

(defun extract (data &rest keys)
  "Collect the output DYNA-CLC:VAL for key in KEYS on each item in DATA."
  (loop for element in data
	collect (mapcar (lambda (k) (val element k)) keys)))

(defun filter-val (data field value &key (contains nil))
  "Get the items in DATA with FIELD matching VALUE. If :contains is nil (the default) look for exact matches,
else so a substring search."
  (remove-if-not
   (lambda (elem)
     (if contains
	 (search value (val elem field))
	 (equal (val elem field) value)))
   data))

(defun filter-vals (data filters)
  "Make successive calls to DYNA-CLC:FILTER-VAL applying each element in FILTERS.
The expected format is a list of pairs (name value). If value contains a \"*\" character then make the call
with :contains t. This means you can't use this to search for a literal \"*\". "
  (if filters
      (progn
	(let ((current (car filters))
	      (rest (cdr filters)))
	  (destructuring-bind (field value) current
	    (filter-vals
	     (filter-val data field (remove #\* value) :contains (search "*" value))
	     rest))))
      data))
