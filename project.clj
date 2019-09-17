(defproject metabase/db2-driver "1.0.0"
  :min-lein-version "2.5.0"

  :profiles
  {:provided
   {:dependencies [
     [metabase-core "1.0.0-SNAPSHOT"]
     [com.jrt/jt400 "1.0"]
   ]}

   :uberjar
   {:auto-clean    true
    :aot :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "db2.metabase-driver.jar"}})
