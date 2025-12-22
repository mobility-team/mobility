from dataclasses import dataclass, field, fields, asdict
from typing import List, Union

Number = Union[int, float]    

@dataclass(frozen=True)
class Parameter:
    name: str
    name_fr: str
    value: Number | bool
    description: str
    parameter_type: type
    #default_value: Number | bool
    possible_values: List[float] | List[int] | tuple = None
    min_value: Number = None
    max_value: Number = None
    unit: str = None
    interval: Number = None
    source_default: str = ""
    parameter_role: str = ""
    


    # def get(self):
    #     """Return parameter value."""
    #     val = self.default_value
    #     self._validate(val)
    #     return val

    def set(self, new_value):
        self.value = new_value

    def validate(self):
        if self.value is None: #todo: improve this!
            return None
        if self.parameter_type is not None:
            if not isinstance(self.value, self.parameter_type):
                t = type(self.value)
                raise TypeError(f"Parameter '{self.name}' must be {self.parameter_type} (currently {t}).")

        if self.min_value is not None and self.value < self.min_value:
            raise ValueError(
                f"Parameter '{self.name}' below minimum {self.min_value}"
            )

        if self.max_value is not None and self.value > self.max_value:
            raise ValueError(
                f"Parameter '{self.name}' above maximum {self.max_value}"
            )

    def __repr__(self):
        unit_str = f" [{self.unit}]" if self.unit else ""
        return f"<Parameter {self.name}={self.value}{unit_str}>"


    def get_values_for_sensitivity_analyses(self, i_max=10):
        value = self.value
        print(self)
        print(value)
        values = [value]
        if self.interval is None:
            raise ValueError("To run a sensitivity analysis, interval must be specified in the parameter configuration")
        i = 0
        if self.max_value is not None:
            print("hop")
            while i < i_max and value < self.max_value:
                value += self.interval
                values.append(round(value,3))
                i += 1
        else:
            while i < i_max:
                value += self.interval
                values.append(round(value,3))                
                i += 1
        value = self.value
        i = 0
        
        if self.min_value is not None:
            while i < i_max and value > self.min_value:
                value -= self.interval
                print(i, value)
                values.append(round(value,3))
                i += 1
        else:
            while i < i_max:
                value -= self.interval
                values.append(round(value,3))                
                i += 1
                
        values.sort()
        print(values)
        return values
    
@dataclass    
class ParameterSet:
    parameters : dict = field(init=False, compare=False)    
    
    def validate(self):
        for param in fields(self)[1:]:
            print(self.parameters)
            param_name = "param_" + param.name
            print(param_name)
            self.parameters[param_name].validate()
        self._validate_param_interdependency()
            
    def _validate_param_interdependency(self):
        pass

    def to_hashable_dict(self) -> dict:
        """Return JSON-serializable values only."""
        return asdict(self)