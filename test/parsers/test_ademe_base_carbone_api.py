import pytest
from mobility.parsers.ademe_base_carbone import get_emissions_factor

def test_ademe_base_carbone_api():
    """Try to get an emissions factor value from the ADEME Base carbone API"""
    emissions_factor = get_emissions_factor("27970")
    assert str(emissions_factor) == "0.218"
    
