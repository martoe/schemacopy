package at.bxm.dbtools.schemacopy;

/**
 * Enforces the correct package dependency within eclipse
 * 
 * 1. install AJDT http://www.eclipse.org/ajdt/ (Update site: http://download.eclipse.org/tools/ajdt/37/update)
 * 2. rightclick project -> configure -> COnvert to AspectJ project
 * 3. in the test-Sourcetree, create an aspect (New -> other -> Aspect)
 */
public aspect PackagePolicyEnforcement {

	pointcut root() : within(at.bxm.dbtools.schemacopy..*);
  pointcut rootCall() : call(* at.bxm.dbtools.schemacopy..*.*(..));
  pointcut table() : within(at.bxm.dbtools.schemacopy.table..*);
  pointcut tableCall() : call(* at.bxm.dbtools.schemacopy.table..*.*(..));
	pointcut sequence() : within(at.bxm.dbtools.schemacopy.sequence..*);
  pointcut sequenceCall() : call(* at.bxm.dbtools.schemacopy.sequence..*.*(..));
	pointcut run() : within(at.bxm.dbtools.schemacopy.run..*);
  pointcut runCall() : call(* at.bxm.dbtools.schemacopy.run..*.*(..));

  declare error : table() && runCall() : "'table' package must not access 'run' package";
  declare error : table() && sequenceCall() : "'table' package must not access 'sequence' package";
  declare error : sequence() && runCall() : "'sequence' package must not access 'run' package";
  declare error : sequence() && tableCall() : "'sequence' package must not access 'table' package";

}
