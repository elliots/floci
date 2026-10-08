#!/usr/bin/env bats

setup() {
    load 'test_helper/common-setup'
    TF_FIXTURE_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")/elasticache-users-default-subnet-tf" && pwd)"
    TF_RESOURCES_DIR="$BATS_TEST_TMPDIR/terraform"
    mkdir -p "$TF_RESOURCES_DIR"
    cp "$TF_FIXTURE_DIR"/*.tf "$TF_RESOURCES_DIR/"
}

teardown() {
    if [ -f "$TF_RESOURCES_DIR/terraform.tfstate" ]; then
        terraform -chdir="$TF_RESOURCES_DIR" destroy -var="endpoint=$FLOCI_ENDPOINT" -input=false -auto-approve -no-color
    fi
}

check_lifecycles() {
    sed "s/~> 6.0/~> $1.0/" "$TF_FIXTURE_DIR/provider.tf" > "$TF_RESOURCES_DIR/provider.tf"
    run terraform -chdir="$TF_RESOURCES_DIR" init -input=false -no-color
    assert_success
    for cycle in 1 2; do
        run terraform -chdir="$TF_RESOURCES_DIR" apply -var="endpoint=$FLOCI_ENDPOINT" -input=false -auto-approve -no-color
        assert_success
        run terraform -chdir="$TF_RESOURCES_DIR" plan -var="endpoint=$FLOCI_ENDPOINT" -input=false -no-color -detailed-exitcode
        assert_success
        assert_output --partial "No changes"
        run terraform -chdir="$TF_RESOURCES_DIR" destroy -var="endpoint=$FLOCI_ENDPOINT" -input=false -auto-approve -no-color
        assert_success
    done
}

@test "AWS provider v5: ElastiCache tags, user groups, associations and default subnet, twice without drift" {
    check_lifecycles 5
}

@test "AWS provider v6: ElastiCache tags, user groups, associations and default subnet, twice without drift" {
    check_lifecycles 6
}
