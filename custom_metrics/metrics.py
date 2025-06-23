class ExecutionAccuracyMetric(BaseMetric):
    def __init__(self, db_dir: str):
        if not os.path.isdir(db_dir):
            raise ValueError(f"O diretório do banco de dados '{db_dir}' não existe.")
        self.db_dir = db_dir
        self.threshold = 1.0

    def measure(self, test_case: LLMTestCase) -> float:
        db_id = next((ctx.split(':')[1] for ctx in test_case.context if ctx.startswith("db_id:")), None)
        if not db_id:
            raise ValueError("O contexto do test_case deve conter 'db_id:<id>'")

        db_path = os.path.join(self.db_dir, db_id, f"{db_id}.sqlite")
        if not os.path.exists(db_path):
            self.reason = f"Falha: Banco de dados não encontrado em '{db_path}'."
            self.score = 0.0
            return self.score

        try:
            actual_results = self._execute_query(db_path, test_case.actual_output)
        except Exception as e:
            self.reason = f"Erro ao executar a consulta gerada: {e}"
            self.score = 0.0
            return self.score

        try:
            expected_results = self._execute_query(db_path, test_case.expected_output)
        except Exception as e:
            self.reason = f"Erro ao executar a consulta ground-truth: {e}"
            self.score = 0.0
            return self.score

        if set(actual_results) == set(expected_results):
            self.reason = "Sucesso: Resultados idênticos."
            self.score = 1.0
        else:
            self.reason = "Falha: Resultados diferentes."
            self.score = 0.0
        return self.score

    def _execute_query(self, db_path: str, query: str):
        with sqlite3.connect(db_path) as conn:
            return conn.cursor().execute(query).fetchall()

    async def a_measure(self, test_case: LLMTestCase, **kwargs) -> float:
        return self.measure(test_case)

    def is_successful(self) -> bool:
        return self.score >= self.threshold

