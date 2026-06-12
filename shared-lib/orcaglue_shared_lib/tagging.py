import pulumi


def add_default_tags(args: pulumi.ResourceTransformationArgs) -> pulumi.ResourceTransformationResult:
    # Globally applied tags
    default_tags = {
        "umccr-org:Product": "OrcaGlue",
        "umccr-org:Creator": "Pulumi",
        "umccr-org:Service": "OrcaGlue",
        "umccr-org:Source": "https://github.com/umccr/orcaglue",
        "ManagedBy": "Pulumi",
        "Project": pulumi.get_project()
    }

    if "tags" in args.props:
        current_tags = args.props.get("tags") or {}
        args.props["tags"] = {**default_tags, **current_tags}
        return pulumi.ResourceTransformationResult(props=args.props, opts=args.opts)

    return None


def register_global_tags():
    pulumi.runtime.register_stack_transformation(add_default_tags)
