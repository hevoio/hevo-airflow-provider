fragment branchProtection on BranchProtectionRule {
  allowsDeletions
  allowsForcePushes
  creator {
    login
  }
  id
  isAdminEnforced
  requiredStatusCheckContexts
  requiredApprovingReviewCount
  requiresApprovingReviews
  requiresCodeOwnerReviews
  requiresStatusChecks
  restrictsPushes
  restrictsReviewDismissals
  dismissesStaleReviews
  pattern
}

mutation addBranchProtection($repositoryId:ID!, $branchPattern:String!, $requiredStatusChecks:[String!]) {
  createBranchProtectionRule(input: {
    allowsDeletions: false
    allowsForcePushes:false
    dismissesStaleReviews:true
    isAdminEnforced:false
    pattern: $branchPattern
    repositoryId: $repositoryId
    requiresApprovingReviews:true
    requiresLinearHistory:true
    requiredApprovingReviewCount:1
    requiresCodeOwnerReviews:true
    requiredStatusCheckContexts:$requiredStatusChecks
    requiresStatusChecks:true
    restrictsReviewDismissals:false
  }) {
    branchProtectionRule {
      ...branchProtection
    }
  }
}

query getRepositoryId($owner:String!, $repo:String!) {
  repository (name: $repo, owner: $owner)  {
        id
  }
}