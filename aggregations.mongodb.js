// Seleccionar la base de datos a usar

use('Fasecolda');

// Agregacion por mes y año

db.datos.aggregate([
  {
    $group: {
      _id: { mes: "$mes", periodo: "$año" },
      total_numero_empresas: { $sum: { $toInt: "$numero_empresas" } },
      total_trabajadores_dependientes: { $sum: { $toInt: "$trabajadores_dependientes" } },
      total_trabajadores_independientes: { $sum: { $toInt: "$trabajadores_independientes" } },
      total_trabajadores: { $sum: { $toInt: "$total_trabajadores" } },
      total_accidentes_trabajo: { $sum: { $toInt: "$accidentes_trabajo" } },
      total_enfermedad_laboral: { $sum: { $toInt: "$enfermedad_laboral" } },
      total_muertes_accidentes_trabajo: { $sum: { $toInt: "$muertes_accidentes_trabajo" } },
      total_muertes_enfermedad_laboral: { $sum: { $toInt: "$muertes_enfermedad_laboral" } },
      total_muertes: { $sum: { $toInt: "$total_muertes" } },
      total_pensiones: { $sum: { $toInt: "$total_pensiones" } },
      total_indemnizaciones_pagadas: { $sum: { $toInt: "$total_indemnizaciones_pagadas" } }
    }
  },
  {
    $out: "DB_periodo_mes"
  }
]);

// Agregacion por mes, año y ARL

db.datos.aggregate([
  {
    $group: {
      _id: { mes: "$mes", periodo: "$año", arl: "$ARL" },
      total_numero_empresas: { $sum: { $toInt: "$numero_empresas" } },
      total_trabajadores_dependientes: { $sum: { $toInt: "$trabajadores_dependientes" } },
      total_trabajadores_independientes: { $sum: { $toInt: "$trabajadores_independientes" } },
      total_trabajadores: { $sum: { $toInt: "$total_trabajadores" } },
      total_accidentes_trabajo: { $sum: { $toInt: "$accidentes_trabajo" } },
      total_enfermedad_laboral: { $sum: { $toInt: "$enfermedad_laboral" } },
      total_muertes_accidentes_trabajo: { $sum: { $toInt: "$muertes_accidentes_trabajo" } },
      total_muertes_enfermedad_laboral: { $sum: { $toInt: "$muertes_enfermedad_laboral" } },
      total_muertes: { $sum: { $toInt: "$total_muertes" } },
      total_pensiones: { $sum: { $toInt: "$total_pensiones" } },
      total_indemnizaciones_pagadas: { $sum: { $toInt: "$total_indemnizaciones_pagadas" } }
    }
  },
  {
    $out: "DB_periodo_mes_arl"
  }
]);

// Agregacion por mes, año, ARL y departamento

db.datos.aggregate([
  {
    $group: {
      _id: { mes: "$mes", periodo: "$año", arl: "$ARL", departamento: "$departamento" },
      total_numero_empresas: { $sum: { $toInt: "$numero_empresas" } },
      total_trabajadores_dependientes: { $sum: { $toInt: "$trabajadores_dependientes" } },
      total_trabajadores_independientes: { $sum: { $toInt: "$trabajadores_independientes" } },
      total_trabajadores: { $sum: { $toInt: "$total_trabajadores" } },
      total_accidentes_trabajo: { $sum: { $toInt: "$accidentes_trabajo" } },
      total_enfermedad_laboral: { $sum: { $toInt: "$enfermedad_laboral" } },
      total_muertes_accidentes_trabajo: { $sum: { $toInt: "$muertes_accidentes_trabajo" } },
      total_muertes_enfermedad_laboral: { $sum: { $toInt: "$muertes_enfermedad_laboral" } },
      total_muertes: { $sum: { $toInt: "$total_muertes" } },
      total_pensiones: { $sum: { $toInt: "$total_pensiones" } },
      total_indemnizaciones_pagadas: { $sum: { $toInt: "$total_indemnizaciones_pagadas" } }
    }
  },
  {
    $out: "DB_periodo_mes_arl_departamento"
  }
]);

// Agregacion por mes, año, ARL, departamento y sector económico

db.datos.aggregate([
  {
    $group: {
      _id: { mes: "$mes", periodo: "$año", arl: "$ARL", departamento: "$departamento", sec_econo: "$sect_eco" },
      total_numero_empresas: { $sum: { $toInt: "$numero_empresas" } },
      total_trabajadores_dependientes: { $sum: { $toInt: "$trabajadores_dependientes" } },
      total_trabajadores_independientes: { $sum: { $toInt: "$trabajadores_independientes" } },
      total_trabajadores: { $sum: { $toInt: "$total_trabajadores" } },
      total_accidentes_trabajo: { $sum: { $toInt: "$accidentes_trabajo" } },
      total_enfermedad_laboral: { $sum: { $toInt: "$enfermedad_laboral" } },
      total_muertes_accidentes_trabajo: { $sum: { $toInt: "$muertes_accidentes_trabajo" } },
      total_muertes_enfermedad_laboral: { $sum: { $toInt: "$muertes_enfermedad_laboral" } },
      total_muertes: { $sum: { $toInt: "$total_muertes" } },
      total_pensiones: { $sum: { $toInt: "$total_pensiones" } },
      total_indemnizaciones_pagadas: { $sum: { $toInt: "$total_indemnizaciones_pagadas" } }
    }
  },
  {
    $out: "DB_periodo_mes_arl_departamento_sect_eco"
  }
]);