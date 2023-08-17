from datetime import date, timedelta, datetime
from random import shuffle

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

# Read only input data paths
INPUT_DATA_PATH = "/data/input/b5/eo-01"

# Auxillary input data paths
AUX_DATA_PATH = "/data/outputs/auxillary_data"
aoi_filepath = AUX_DATA_PATH + "/vector/aoi.gpkg"
datacube_folderpath = AUX_DATA_PATH + "/grid"
datacube_filepath = datacube_folderpath + "/datacube-definition.prj"
dem_folderpath = AUX_DATA_PATH + "/dem"
wvdb = AUX_DATA_PATH + "/wvdb"
endmember_filepath = AUX_DATA_PATH + "/endmember/hostert-2003.txt"

# Output data paths
OUTPUTS_DATA_PATH = "/data/outputs"
allowed_tiles_filepath = OUTPUTS_DATA_PATH + "/allowed_tiles.txt"
masks_folderpath = OUTPUTS_DATA_PATH + "/masks"
queue_filepath = OUTPUTS_DATA_PATH + "/queue.txt"
ard_folderpath = OUTPUTS_DATA_PATH + "/level2_ard"
trends_folderpath = OUTPUTS_DATA_PATH + "/trends"
mosaic_folderpath = OUTPUTS_DATA_PATH + "/mosaic"
tests_folderpath = OUTPUTS_DATA_PATH + "/check-results"

# Query parameters
sensors_level1 = "LT04,LT05,LE07,S2A"
start_date = date(1984, 1, 1)
end_date = date(2006, 12, 31)
daterange = start_date.strftime("%Y%m%d") + "," + end_date.strftime("%Y%m%d")
mask_resolution = 30

# Run parameters
num_of_tiles = 28
parallel_factor = 2794  # Parallel factor is how many images are to be processed
num_of_filters = 10
# You have to assert that the number of pyramids tasks per tile is
# smaller than the number of the actual filters
num_of_pyramid_tasks_per_tile = 10

# Kubernetes config: namespace, resources, volume and volume_mounts
namespace = "default"

compute_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "2000m",
        "memory": "1.5Gi",
        },
    limits={
        "cpu": "2000m",
        "memory": "4.5Gi",
    }
)

dataset_volume = k8s.V1Volume(
    name="eo-data",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="fonda-datasets"
    ),
)

dataset_volume_mount = k8s.V1VolumeMount(
    name="eo-data", mount_path="/data/input", sub_path=None, read_only=True
)

outputs_volume = k8s.V1Volume(
    name="outputs-data",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="force-airflow"
    ),
)

outputs_volume_mount = k8s.V1VolumeMount(
    name="outputs-data", mount_path=OUTPUTS_DATA_PATH, sub_path=None, read_only=False
)

security_context = k8s.V1SecurityContext(run_as_user=0)

experiment_affinity = {
    "nodeAffinity": {
        # requiredDuringSchedulingIgnoredDuringExecution means in order
        # for a pod to be scheduled on a node, the node must have the
        # specified labels. However, if labels on a node change at
        # runtime such that the affinity rules on a pod are no longer
        # met, the pod will still continue to run on the node.
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            "key": "usedby",
                            "operator": "In",
                            # The label key's value that pods can be scheduled
                            # on.
                            "values": [
                                "vasilis",
                            ],
                        }
                    ]
                }
            ]
        }
    }
}

# DAG
default_args = {
    "owner": "FONDA S1",
    "depends_on_past": False,
    "email": ["vasilis.bountris@informatik.hu-berlin.de"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "force",
    default_args=default_args,
    description="Airflow implementation of a FORCE workflow",
    schedule_interval="@once",
    start_date=datetime(2022, 3, 25),
    tags=["force"],
    max_active_runs=1,
) as dag:

    generate_allowed_tiles = KubernetesPodOperator(
        name="generate_allowed_tiles",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        labels={"workflow": "force", "logging": "elastic"},
        task_id="generate_allowed_tiles",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"force-tile-extent {aoi_filepath} {datacube_folderpath} {allowed_tiles_filepath}"
        ],
        security_context=security_context,
        container_resources=compute_resources,
        volumes=[dataset_volume, outputs_volume],
        volume_mounts=[dataset_volume_mount, outputs_volume_mount],
        get_logs=True,
        reattach_on_restart=False,
        is_delete_operator_pod=True,
        affinity=experiment_affinity,
        dag=dag,
    )

    generate_analysis_mask = KubernetesPodOperator(
        name="generate_analysis_mask",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        labels={"workflow": "force", "logging": "elastic"},
        task_id="generate_analysis_mask",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"mkdir -p {masks_folderpath} && cp {datacube_folderpath}/datacube-definition.prj {masks_folderpath} && force-cube {aoi_filepath} {masks_folderpath} rasterize {mask_resolution}"
        ],
        security_context=security_context,
        container_resources=compute_resources,
        volumes=[dataset_volume, outputs_volume],
        volume_mounts=[dataset_volume_mount, outputs_volume_mount],
        get_logs=True,
        reattach_on_restart=False,
        is_delete_operator_pod=True,
        affinity=experiment_affinity,
        dag=dag,
    )

    prepare_level2 = KubernetesPodOperator(
        name="prepare_level2",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        labels={"workflow": "force", "logging": "elastic"},
        task_id="prepare_level2",
        cmds=["/bin/sh", "-c"],
        arguments=[
            f"""
        # wget -O {queue_filepath} https://box.hu-berlin.de/f/8cbd80805d484be1b91a/?dl=1
        mkdir -p /data/outputs/queue_files
        split -a 4 -l$((`wc -l < {queue_filepath}`/{parallel_factor})) --numeric-suffixes=0 {queue_filepath} /data/outputs/queue_files/queue_ --additional-suffix=.txt
        mkdir -p /data/outputs/param_files
        mkdir -p /data/outputs/level2_ard
        mkdir -p /data/outputs/level2_log
        mkdir -p /data/outputs/level2_tmp
        force-parameter . LEVEL2 0
        mv LEVEL2-skeleton.prm $PARAM
        # read grid definition
        CRS=$(sed '1q;d' $CUBEFILE)
        ORIGINX=$(sed '2q;d' $CUBEFILE)
        ORIGINY=$(sed '3q;d' $CUBEFILE)
        TILESIZE=$(sed '6q;d' $CUBEFILE)
        BLOCKSIZE=$(sed '7q;d' $CUBEFILE)
        # set parameters
        # sed -i "/^PARALLEL_READS /cPARALLEL_READS = TRUE" $PARAM
        # sed -i "/^DELAY /cDELAY = 2" $PARAM
        sed -i "/^NPROC /cNPROC = 1" $PARAM
        sed -i "/^DIR_LEVEL2 /cDIR_LEVEL2 = /data/outputs/level2_ard/" $PARAM
        sed -i "/^DIR_LOG /cDIR_LOG = /data/outputs/level2_log/" $PARAM
        sed -i "/^DIR_TEMP /cDIR_TEMP = /data/outputs/level2_tmp/" $PARAM
        sed -i "/^FILE_DEM /cFILE_DEM = $DEM/crete_srt-master.vrt" $PARAM
        sed -i "/^DIR_WVPLUT /cDIR_WVPLUT = $WVDB" $PARAM
        sed -i "/^FILE_TILE /cFILE_TILE = $TILE" $PARAM
        sed -i "/^TILE_SIZE /cTILE_SIZE = $TILESIZE" $PARAM
        sed -i "/^BLOCK_SIZE /cBLOCK_SIZE = $BLOCKSIZE" $PARAM
        sed -i "/^ORIGIN_LON /cORIGIN_LON = $ORIGINX" $PARAM
        sed -i "/^ORIGIN_LAT /cORIGIN_LAT = $ORIGINY" $PARAM
        sed -i "/^PROJECTION /cPROJECTION = $CRS" $PARAM
        sed -i "/^NTHREAD /cNTHREAD = 2" $PARAM
            """
        ],
        security_context=security_context,
        container_resources=compute_resources,
        volumes=[dataset_volume, outputs_volume],
        volume_mounts=[dataset_volume_mount, outputs_volume_mount],
        env_vars={
            "CUBEFILE": datacube_filepath,
            "DEM": dem_folderpath,
            "WVDB": wvdb,
            "TILE": allowed_tiles_filepath,
            "NTHREAD": "2",
            "PARAM": "/data/outputs/param_files/ard.prm",
        },
        get_logs=True,
        affinity=experiment_affinity,
        dag=dag,
    )

    preprocess_level2_tasks = []

    # Randomize task order through their indices, because in Airflow
    # they run in the same order they have on the preprocess_level2_tasks list
    preprocess_level2_tasks_indices = [i for i in range(parallel_factor)]
    shuffle(preprocess_level2_tasks_indices)

    for i in preprocess_level2_tasks_indices:
        index = f"{i:04d}"
        preprocess_level2_task = KubernetesPodOperator(
            name="preprocess_level2_" + index,
            namespace=namespace,
            image="davidfrantz/force:3.6.5",
            labels={"workflow": "force", "logging": "elastic"},
            task_id="preprocess_level2_" + index,
            cmds=["/bin/sh", "-c"],
            arguments=[
                """\
            cp $GLOBAL_PARAM $PARAM
            # mkdir -p /data/outputs/fake
            # mkdir -p /data/outputs/fake/$FAKE_INDEX
            # sed -i "/^DIR_LEVEL2 /cDIR_LEVEL2 = /data/outputs/fake/$FAKE_INDEX" $PARAM
            sed -i "/^FILE_QUEUE /cFILE_QUEUE = $QUEUE_FILE" $PARAM
            if grep -q DONE "$QUEUE_FILE"; then
              exit 0
            fi
            echo YO
            force-l2ps `(awk '{print $1; exit}' $QUEUE_FILE)` $PARAM
            sed -i "s/QUEUED/DONE/g" $QUEUE_FILE
                """
            ],
            security_context=security_context,
            container_resources=compute_resources,
            volumes=[dataset_volume, outputs_volume],
            volume_mounts=[dataset_volume_mount, outputs_volume_mount],
            env_vars={
                "GLOBAL_PARAM": "/data/outputs/param_files/ard.prm",
                "PARAM": f"/data/outputs/param_files/ard_{index}.prm",
                "QUEUE_FILE": f"/data/outputs/queue_files/queue_{index}.txt",
                "FAKE_INDEX": f"{index}",
            },
            get_logs=True,
            pool="restricted_pool",
            reattach_on_restart=False,
            is_delete_operator_pod=True,
            affinity=experiment_affinity,
            dag=dag,
            execution_timeout=timedelta(minutes=30),
            retry_delay=timedelta(minutes=20),
        )
        preprocess_level2_tasks.append(preprocess_level2_task)

    prepare_tsa = KubernetesPodOperator(
        name="prepape_tsa",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        labels={"workflow": "force"},
        task_id="prepare_tsa",
        cmds=["/bin/sh", "-c"],
        arguments=[
            """\
        force-parameter . TSA 0
        mv TSA-skeleton.prm $PARAM
        mkdir -p $TRENDS_FOLDER

        # paths
        sed -i "/^DIR_LOWER /cDIR_LOWER = $ARD_FOLDER" $PARAM
        sed -i "/^DIR_HIGHER /cDIR_HIGHER = $TRENDS_FOLDER" $PARAM
        sed -i "/^DIR_MASK /cDIR_MASK = $MASKS_FOLDER" $PARAM
        sed -i "/^FILE_ENDMEM /cFILE_ENDMEM = $ENDMEMBER" $PARAM
        sed -i "/^FILE_TILE /cFILE_TILE = $TILE" $PARAM

        # threading
        sed -i "/^NTHREAD_READ /cNTHREAD_READ = 1" $PARAM
        sed -i "/^NTHREAD_COMPUTE /cNTHREAD_COMPUTE = 2" $PARAM
        sed -i "/^NTHREAD_WRITE /cNTHREAD_WRITE = 1" $PARAM

        # resolution
        sed -i "/^RESOLUTION /cRESOLUTION = 30" $PARAM

        # sensors
        sed -i "/^SENSORS /cSENSORS = LND04 LND05 LND07" $PARAM

        # date range
        sed -i "/^DATE_RANGE /cDATE_RANGE = $START_DATE $END_DATE" $PARAM

        # spectral index
        sed -i "/^INDEX /cINDEX = SMA" $PARAM

        # interpolation
        sed -i "/^INT_DAY /cINT_DAY = 8" $PARAM
        sed -i "/^OUTPUT_TSI /cOUTPUT_TSI = TRUE" $PARAM

        # polar metrics
        sed -i "/^POL /cPOL = VPS VBL VSA" $PARAM
        sed -i "/^OUTPUT_POL /cOUTPUT_POL = TRUE" $PARAM
        sed -i "/^OUTPUT_TRO /cOUTPUT_TRO = TRUE" $PARAM
        sed -i "/^OUTPUT_CAO /cOUTPUT_CAO = TRUE" $PARAM

        cp $PARAM /data/outputs/param_files/

        echo "DONE"
            """
        ],
        security_context=security_context,
        container_resources=compute_resources,
        volumes=[dataset_volume, outputs_volume],
        volume_mounts=[dataset_volume_mount, outputs_volume_mount],
        env_vars={
            "CUBEFILE": datacube_filepath,
            "DEM": dem_folderpath,
            "WVDB": wvdb,
            "TILE": allowed_tiles_filepath,
            "NTHREAD": "2",
            "PARAM": "tsa.prm",
            "ENDMEMBER": endmember_filepath,
            "ARD_FOLDER": ard_folderpath,
            "TRENDS_FOLDER": trends_folderpath,
            "MASKS_FOLDER": masks_folderpath,
            "AOI_PATH": aoi_filepath,
            "START_DATE": start_date.isoformat(),
            "END_DATE": end_date.isoformat(),
        },
        get_logs=True,
        reattach_on_restart=False,
        is_delete_operator_pod=True,
        affinity=experiment_affinity,
        dag=dag,
    )

    tsa_tasks = []
    for i in range(2, num_of_tiles + 2):
        index = f"{i:03d}"
        tsa_task = KubernetesPodOperator(
            name="tsa_task_" + index,
            namespace=namespace,
            image="davidfrantz/force:3.6.5",
            labels={"workflow": "force"},
            task_id="tsa_task_" + index,
            cmds=["/bin/bash", "-c"],
            arguments=[
                f"""\
              echo "STARTING TIME SERIES ANALYSIS"
              cp $GLOBAL_PARAM $PARAM
              # Get the corresponding line from the allowed tiles file
              TILE=`sed '{index}q;d' $TILE_FILE`
              X=${{TILE:1:4}}
              Y=${{TILE:7:11}}
              sed -i "/^BASE_MASK /cBASE_MASK = aoi.tif" $PARAM
              sed -i "/^X_TILE_RANGE /cX_TILE_RANGE = $X $X" $PARAM
              sed -i "/^Y_TILE_RANGE /cY_TILE_RANGE = $Y $Y" $PARAM
              force-higher-level $PARAM
              echo "DONE"

              # Push results to xcom
              mkdir -p /airflow/xcom/

              # Find *.tif files and store them in a list of files
              cd /data/outputs/trends/$TILE
              files=`find *.tif | tr '\n' ','`
              # Add Brackets
              files='['$files']'
              # Make json
              echo '{{"tile":"'$TILE'", "files":"'$files'"}}'
              echo '{{"tile":"'$TILE'", "files":"'$files'"}}' > /airflow/xcom/return.json

                  """
            ],
            security_context=security_context,
            container_resources=compute_resources,
            volumes=[dataset_volume, outputs_volume],
            volume_mounts=[dataset_volume_mount, outputs_volume_mount],
            do_xcom_push=True,
            env_vars={
                "GLOBAL_PARAM": "/data/outputs/param_files/tsa.prm",
                "PARAM": f"/data/outputs/param_files/tsa_{index}.prm",
                "TILE_FILE": allowed_tiles_filepath,
            },
            get_logs=True,
            affinity=experiment_affinity,
            dag=dag,
        )
        tsa_tasks.append(tsa_task)

    pyramid_tasks_per_tile = []
    for i in range(2, num_of_tiles + 2):
        # TODO: pyramids could and should be tried in some form of batches
        pyramid_tasks_in_tile = []
        for j in range(num_of_filters):

            pyramid_task_index = i * num_of_filters + j
            file_index = str(j + 1)
            tile_index = f"{i:03d}"
            index = f"{pyramid_task_index:03d}"
            pyramid_task = KubernetesPodOperator(
                name="pyramid_task_" + index,
                namespace=namespace,
                image="davidfrantz/force:3.6.5",
                labels={"workflow": "force"},
                task_id="pyramid_task_" + index,
                cmds=["/bin/bash", "-c"],
                arguments=[
                    f"""\
                            TILE=\"{{{{ task_instance.xcom_pull('tsa_task_{tile_index}')[\"tile\"] }}}}\"
                            FILES=\"{{{{ task_instance.xcom_pull('tsa_task_{tile_index}')[\"files\"] }}}}\"
                            CHOSEN_FILE=`echo $FILES | sed 's/[][]//g' | cut -d "," -f $FILE_INDEX`
                            FILES_TO_DO="${{TRENDS_FOLDERPATH}}/${{TILE}}/${{CHOSEN_FILE}}"
                            force-pyramid $FILES_TO_DO
                  """
                ],
                security_context=security_context,
                container_resources=compute_resources,
                volumes=[dataset_volume, outputs_volume],
                volume_mounts=[dataset_volume_mount, outputs_volume_mount],
                env_vars={
                    "INDEX": index,
                    "TRENDS_FOLDERPATH": trends_folderpath,
                    "FILE_INDEX": file_index,
                },
                get_logs=True,
                reattach_on_restart=False,
                is_delete_operator_pod=True,
                retry_delay=timedelta(minutes=5),
                affinity=experiment_affinity,
                dag=dag,
            )
            pyramid_tasks_in_tile.append(pyramid_task)
        pyramid_tasks_per_tile.append(pyramid_tasks_in_tile)

    wait_for_trends = KubernetesPodOperator(
        name="wait_for_trends",
        namespace=namespace,
        image="davidfrantz/force:3.6.5",
        labels={"workflow": "force"},
        task_id="wait_for_trends",
        cmds=["/bin/bash", "-c"],
        arguments=[
            """\
            cd $TRENDS_FOLDERPATH
            UNIQUE_BASENAMES=`find . -name '*.tif' -exec basename {} \; | sort | uniq`
            COUNTER=0
            for i in $UNIQUE_BASENAMES
            do
              mkdir -p $DATA_FOLDERPATH/$COUNTER
              FILES_TO_MOVE=`find . -name $i | cut -c 2-`
              for FILE in $FILES_TO_MOVE
              do
                TILE=`dirname $FILE`
                echo $TILE
                echo $FILE
                mkdir -p $DATA_FOLDERPATH/$COUNTER$TILE
                ln .$FILE $DATA_FOLDERPATH/$COUNTER$FILE
              done
              let COUNTER++
            done
            """
        ],
        security_context=security_context,
        container_resources=compute_resources,
        volumes=[dataset_volume, outputs_volume],
        volume_mounts=[dataset_volume_mount, outputs_volume_mount],
        env_vars={
            "TRENDS_FOLDERPATH": trends_folderpath,
            "DATA_FOLDERPATH": mosaic_folderpath,
        },
        get_logs=True,
        reattach_on_restart=False,
        is_delete_operator_pod=True,
        affinity=experiment_affinity,
        dag=dag,
    )

    mosaic_tasks = []
    for i in range(10):
        index = f"{i:01d}"
        mosaic_task = KubernetesPodOperator(
            name="mosaic_task_" + index,
            namespace=namespace,
            image="davidfrantz/force:3.6.5",
            labels={"workflow": "force"},
            task_id="mosaic_task_" + index,
            cmds=["/bin/bash", "-c"],
            arguments=[
                f"""\
                      force-mosaic $MOSAIC_FOLDERPATH/$INDEX
                  """
            ],
            security_context=security_context,
            container_resources=compute_resources,
            volumes=[dataset_volume, outputs_volume],
            volume_mounts=[dataset_volume_mount, outputs_volume_mount],
            env_vars={
                "INDEX": index,
                "MOSAIC_FOLDERPATH": mosaic_folderpath,
            },
            get_logs=True,
            reattach_on_restart=False,
            is_delete_operator_pod=True,
            affinity=experiment_affinity,
            dag=dag,
        )
        mosaic_tasks.append(mosaic_task)

    check_results = KubernetesPodOperator(
        name="check_results",
        namespace=namespace,
        image="rocker/geospatial:3.6.3",
        labels={"workflow": "force"},
        task_id="check_results",
        cmds=["/bin/sh", "-c"],
        arguments=[
            """\
        mkdir -p $TRENDS_FOLDERPATH/mosaic
        cp $MOSAIC_FOLDERPATH/0/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_TSI.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/1/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VBL-CAO.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/2/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VBL-POL.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/3/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VBL-TRO.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/4/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VPS-CAO.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/5/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VPS-POL.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/6/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VPS-TRO.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/7/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VSA-CAO.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/8/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VSA-POL.vrt $TRENDS_FOLDERPATH/mosaic/
        cp $MOSAIC_FOLDERPATH/9/mosaic/1984-2006_001-365_HL_TSA_LNDLG_SMA_VSA-TRO.vrt $TRENDS_FOLDERPATH/mosaic/
        Rscript $TESTS_FOLDERPATH/test.R $TRENDS_FOLDERPATH/mosaic $TESTS_FOLDERPATH/reference.RData log.log
            """
        ],
        security_context=security_context,
        container_resources={
            "request_cpu": "2000m",
            "request_memory": "10Gi",
        },
        volumes=[dataset_volume, outputs_volume],
        volume_mounts=[dataset_volume_mount, outputs_volume_mount],
        env_vars={
            "TRENDS_FOLDERPATH": trends_folderpath,
            "MOSAIC_FOLDERPATH": mosaic_folderpath,
            "TESTS_FOLDERPATH": tests_folderpath,
        },
        get_logs=True,
        affinity=experiment_affinity,
        dag=dag,
    )

    dag_start = DummyOperator(task_id="Start", dag=dag)
    generate_allowed_tiles.set_upstream(dag_start)
    generate_analysis_mask.set_upstream(dag_start)
    prepare_level2.set_upstream(generate_allowed_tiles)
    prepare_level2.set_upstream(generate_analysis_mask)
    for task in preprocess_level2_tasks:
        task.set_upstream(prepare_level2)
        task.set_downstream(prepare_tsa)

    for tsa_task, pyramid_task_per_tile in zip(tsa_tasks, pyramid_tasks_per_tile):
        tsa_task.set_upstream(prepare_tsa)
        tsa_task.set_downstream(wait_for_trends)
        # Start pyramid batch of pyramid tasks for every tile.
        tsa_task.set_downstream(pyramid_task_per_tile)

    for task in mosaic_tasks:
        task.set_upstream(wait_for_trends)
        task.set_downstream(check_results)
