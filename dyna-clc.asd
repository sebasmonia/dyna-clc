;;;; dynamo.asd

(asdf:defsystem #:dyna-clc
  :description "Access Dynamo via the AWS CLI"
  :author "Sebastian Monia <smonia@outlook.com>"
  :license  "MIT"
  :version "0.0.1"
  :serial t
  :depends-on (#:alexandria
               #:uiop
               #:jonathan)
  :components ((:file "package")
               (:file "dyna-clc")))
