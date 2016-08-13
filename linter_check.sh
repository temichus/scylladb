#!/bin/bash

# linter_check ensures your changes will pass on travis.
# Requires inspektor: pip install inspektor

inspekt --exclude thrift_bindings,cassandra-thrift lint
inspekt_lint_result=$?

inspekt --exclude thrift_bindings,cassandra-thrift indent
inspekt_indent_result=$?

inspekt --exclude thrift_bindings,cassandra-thrift style --max-line-length=200
inspekt_style_result=$?

echo -e "\nlint check command exited with ${inspekt_lint_result}."
echo "indent check command exited with ${inspekt_indent_result}."
echo "style check command exited with ${inspekt_style_result}."

if [ ${inspekt_lint_result} -ne 0 -o ${inspekt_indent_result} -ne 0 -o ${inspekt_style_result} -ne 0 ];
then
    echo "Your changes contain linter errors."
    echo "You can fix these manually, or for style/indent fixes, you can also run"
    echo "inspekt --exclude thrift_bindings,cassandra-thrift indent --fix"
    echo "inspekt --exclude thrift_bindings,cassandra-thrift style --max-line-length=200 --fix"
    echo
    exit 1
fi

echo "Done"
exit 0
