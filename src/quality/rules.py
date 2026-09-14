"""Declarative Spark SQL predicates; no session is created at import time."""

from dataclasses import dataclass
from enum import Enum


class Severity(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class QualityRule:
    name: str
    dataset: str
    description: str
    condition: str
    severity: Severity = Severity.ERROR

    def __post_init__(self):
        object.__setattr__(self, "severity", Severity(self.severity))
        if not all(value.strip() for value in (
            self.name, self.dataset, self.description, self.condition
        )):
            raise ValueError("Rule fields must not be empty")


_DEFINITIONS = {
    "orders": (
        ("order_id_not_null", "Identificador do pedido obrigatório", "order_id IS NOT NULL"),
        ("order_id_positive", "Identificador do pedido positivo", "order_id > 0"),
        ("user_id_not_null", "Identificador do usuário obrigatório", "user_id IS NOT NULL"),
        ("user_id_positive", "Identificador do usuário positivo", "user_id > 0"),
        ("eval_set_allowed", "Conjunto prior, train ou test", "eval_set IN ('prior','train','test')"),
        ("order_number_min", "Número do pedido a partir de um", "order_number >= 1"),
        ("order_dow_range", "Dia da semana entre zero e seis", "order_dow BETWEEN 0 AND 6"),
        ("order_hour_range", "Hora entre zero e 23", "order_hour_of_day BETWEEN 0 AND 23"),
        ("days_since_prior_order_nonnegative", "Intervalo nulo ou não negativo",
         "days_since_prior_order IS NULL OR days_since_prior_order >= 0"),
    ),
    "products": (
        ("product_id_not_null", "Identificador do produto obrigatório", "product_id IS NOT NULL"),
        ("product_id_positive", "Identificador do produto positivo", "product_id > 0"),
        ("product_name_not_null", "Nome obrigatório", "product_name IS NOT NULL"),
        ("product_name_not_blank", "Nome não vazio após trim", "length(trim(product_name)) > 0"),
        ("aisle_id_positive", "Identificador do corredor positivo", "aisle_id > 0"),
        ("department_id_positive", "Identificador do departamento positivo", "department_id > 0"),
    ),
    "aisles": (
        ("aisle_id_not_null", "Identificador do corredor obrigatório", "aisle_id IS NOT NULL"),
        ("aisle_id_positive", "Identificador do corredor positivo", "aisle_id > 0"),
        ("aisle_not_null", "Nome do corredor obrigatório", "aisle IS NOT NULL"),
        ("aisle_not_blank", "Nome não vazio após trim", "length(trim(aisle)) > 0"),
    ),
    "departments": (
        ("department_id_not_null", "Identificador obrigatório", "department_id IS NOT NULL"),
        ("department_id_positive", "Identificador positivo", "department_id > 0"),
        ("department_not_null", "Nome obrigatório", "department IS NOT NULL"),
        ("department_not_blank", "Nome não vazio após trim", "length(trim(department)) > 0"),
    ),
}

_ORDER_PRODUCTS = (
    ("order_id_not_null", "Identificador do pedido obrigatório", "order_id IS NOT NULL"),
    ("order_id_positive", "Identificador do pedido positivo", "order_id > 0"),
    ("product_id_not_null", "Identificador do produto obrigatório", "product_id IS NOT NULL"),
    ("product_id_positive", "Identificador do produto positivo", "product_id > 0"),
    ("add_to_cart_order_min", "Posição no carrinho a partir de um", "add_to_cart_order >= 1"),
    ("reordered_allowed", "Recompra igual a zero ou um", "reordered IN (0,1)"),
)

RULES = {
    dataset: tuple(QualityRule(name, dataset, description, condition)
                   for name, description, condition in definitions)
    for dataset, definitions in {
        **_DEFINITIONS,
        "order_products_prior": _ORDER_PRODUCTS,
        "order_products_train": _ORDER_PRODUCTS,
    }.items()
}


def get_rules(dataset: str) -> tuple[QualityRule, ...]:
    rules = RULES.get(dataset)
    if not rules:
        raise ValueError(f"No quality rules configured for dataset: {dataset}")
    return rules
