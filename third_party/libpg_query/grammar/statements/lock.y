LockStmt:
			LOCK_P opt_table relation_expr opt_lock
				{
					PGLockStmt *n = makeNode(PGLockStmt);
					n->relation = $3;
					n->mode = $4;
					$$ = (PGNode *) n;
				}
		;

opt_lock:	IN_P lock_type MODE				{ $$ = $2; }
			| /* EMPTY */					{ $$ = LCS_FORUPDATE; }

lock_type:	SHARE							{ $$ = PG_LCS_FORSHARE; }
			| EXCLUSIVE						{ $$ = LCS_FORUPDATE; }
