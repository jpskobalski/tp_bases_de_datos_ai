# Evaluación del componente vectorial

El scoring online propuesto trabaja con variables numéricas y categóricas estructuradas. Por ese motivo, una base vectorial no agrega valor al camino crítico y no se incluye en la implementación mínima.

Una extensión futura podría vectorizar evidencia no estructurada asociada a investigaciones, por ejemplo descripciones de contracargos, notas de analistas o documentos aportados por clientes. La búsqueda por similitud ayudaría a encontrar casos históricos parecidos.

Cada vector debería incluir metadatos como `pagador_id`, `decision_id`, fecha, país, tipo de evidencia, estado de revisión y nivel de confidencialidad. Los filtros de autorización deben aplicarse antes de recuperar contenido. Un resultado similar nunca debería modificar automáticamente una decisión: solo serviría como evidencia para revisión humana.
