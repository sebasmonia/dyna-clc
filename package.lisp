;;;; package.lisp

(defpackage #:dyna-clc
  (:nicknames "dclc" :dclc)
  (:use #:common-lisp)
  (:import-from :uiop)
  (:import-from :jonathan)
  (:import-from :alexandria)
  (:export
   #:scan-table
   #:elements-by-key
   #:delete-by-key
   #:upsert-item
   #:dump-json))

(in-package #:dyna-clc)
