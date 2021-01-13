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

;; TODO: check if DELETE-BY-KEYS can be used instead of DELETE-BY-KEY.
;; TODO: check if the same parameter mechanism in this function can be used for ELEMENTS-BY-KEY.
(defun delete-by-keys (table-name key-value-alist profile)
  "Delete the item in TABLE-NAME that matches specified KEY-VALUE-ALIST with the format
(keyName . keyValue). This function works only for tables with a SortKey, else use DELETE-BY-KEY
instead.
See the DYNAMO-OPERATION docs for notes on the PROFILE parameter."
  (let ((key-value-formatted (loop for (key . value) in key-value-alist
                                   collecting (format nil "{\"~a\":{\"S\": \"~a\"}}" key value))))
    (dynamo-operation "delete-item"
                    table-name
                    (apply #'concatenate
                           'string
                           key-value-formatted)
                    profile)))

(defun upsert-item (table-name item-data profile)
  "Upsert an item in TABLE-NAME using ITEM-DATA. If the PartitionKey (and optionally SortKey)
in ITEM-DATA matches an existing element, it will be updated, else you get a new element.
See the DYNAMO-OPERATION docs for notes on the PROFILE parameter."
  (let ((dynamo-formatted (convert-item-to-dynamo item-data)))
    (uiop:with-temporary-file (:stream tmp-stream :pathname tmp-pathname)
      (write-string (jonathan:to-json dynamo-formatted :from :alist) tmp-stream)
      :close-stream
      (dynamo-operation "put-item"
                        table-name
                        (list "--item"
                              (concatenate 'string
                                           "file://"
                                           (uiop:native-namestring tmp-pathname)))
                        profile))))

(defun dump-json ()
)

(defun parse-items-list (aws-json)
  "Use JONATHAN:PARSE to convert the JSON-representation of Dynamo items returned by the AWS CLI to
a (hopefully) Lispier format, that is also more readable in the REPL."
  (let ((jonathan:*null-value* :null)
        (jonathan:*false-value* :false)
        (jonathan:*empty-array-value* :[]))
    (mapcar
     #'parse-single-item
     (gethash "Items"
              (jonathan:parse aws-json :as :hash-table)))))

(defun parse-single-item (item)
  "Parses the contents of the ITEM hash-table to unravel its contents as errrr...plainer objects.
For each element in the table, it is converted to a cons cell ( StringKey . ParsedValue ).
See CONVERT-DYNAMO-ENTITY for details on the individual type conversions."
  (loop for k being the hash-keys in item using (hash-value v)
	collect (cons k
		      (convert-dynamo-entity v))))

(defun convert-dynamo-entity (content)
  "Identify the type of CONTENT and convert it to a Lisp object. \"M\"aps (JSON objects) get a
second call to this same function. \"L\"ists (JS arrays) are handled with a subcall to
PARSE-SINGLE-ITEM.
Some values depend on jonathan's parsing parameters as bound in PARSE-ITEM-LIST (true/false, empty
array)."
  (let ((map-data (gethash "M" content))
	(list-data (gethash "L" content))
	(boolean-data (gethash "BOOL" content))
	(string (gethash "S" content))
	(binary (gethash "B" content))
	(number (gethash "N" content))
	(set-number (gethash "NS" content))
	(set-data (or (gethash "SS" content)
		      (gethash "BS" content))))
    (cond (string string)
	  (boolean-data (if (equal boolean-data "true")
                            t
                            :false))
	  (binary binary)
	  ;; this can be very bad, but I trust the Dynamo output...
	  (number (read-from-string number))
	  (set-number (mapcar #'read-from-string set-number))
	  ;; return as-is
	  (set-data set-data)
          ;; if the list is empty, returns nil...which the empty list
	  (list-data (unless (eq list-data :[])
                       (mapcar #'convert-dynamo-entity list-data)))
	  (map-data (parse-single-item map-data)))))


(defun convert-item-to-dynamo (somewhat-convenient-alist)
  "This function takes a SOMEWHAT-CONVENIENT-ALIST in the format returned by PARSE-SINGLE-ITEM
and turns it back to the original type-prefixed-format. This is necessary for upsert operations."
  (mapcar
   #'convert-to-dynamo-entity
   somewhat-convenient-alist))

(defun convert-to-dynamo-entity (item)
  "Check ITEM to determine its \"Dynamo type\", and return a version prefixed the S, B, M, etc.
that can be JSON-serialized in a way that makes the AWS CLI happy."
  (if (listp item)
      (convert-to-dynamo-entity (cdr item))
      )
  )

(defun is-map (elem)
  "Determines if ELEM should represent a plain JSON object."

  )
