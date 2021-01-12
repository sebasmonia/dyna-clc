;;;; dyna-clc.lisp

(in-package #:dyna-clc)

(defun dynamo-operation (operation table-name parameters-list profile)
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
  (let ((raw-output (dynamo-operation "scan"
				      table-name
				      nil
				      profile)))
    (parse-items-list
     (let ((jonathan:*null-value* :null)
           (jonathan:*false-value* :false)
           (jonathan:*empty-array-value* :[]))
       (jonathan:parse raw-output :as :hash-table)))))

(defun elements-by-key (table-name key-name key-value profile)
  (let* ((params (list "--key-condition-expression"
                       (format nil "~a = :key" key-name)
                       "--expression-attribute-values"
                       (format nil "{\":key\":{\"S\": \"~a\"}}" key-value)))
         (raw-output (dynamo-operation "query"
				      table-name
				      params
				      profile)))
    (parse-items-list
     (let ((jonathan:*null-value* :null)
           (jonathan:*false-value* :false)
           (jonathan:*empty-array-value* :[]))
       (jonathan:parse raw-output :as :hash-table)))))

(defun delete-by-key (table-name key-name key-value profile)
  (dynamo-operation "delete-item"
                    table-name
                    (list "--key"
                          (format nil "{\"~a\":{\"S\": \"~a\"}}"
                                  key-name
                                  key-value))
                    profile))

(defun delete-by-keys (table-name key-value-alist profile)
  (let ((key-value-formatted (loop for (key . value) in key-value-alist
                                   collecting (format nil "{\"~a\":{\"S\": \"~a\"}}" key value))))
    (dynamo-operation "delete-item"
                    table-name
                    (apply #'concatenate
                           'string
                           key-value-formatted)
                    profile)))

(defun upsert-item ()

)

(defun dump-json ()
)


(defun parse-items-list (aws-json)
  (mapcar
   #'parse-single-item
   (gethash "Items" aws-json)))

(defun parse-single-item (item)
  (loop for k being the hash-keys in item using (hash-value v)
	collect (cons k
		      (convert-aws-entity v))))

(defun convert-aws-entity (content)
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
                       (mapcar #'convert-aws-entity list-data)))
	  (map-data (parse-single-item map-data)))))

