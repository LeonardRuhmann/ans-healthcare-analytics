<script setup>
import { computed } from 'vue'
import {
  Chart as ChartJS,
  Title,
  Tooltip,
  Legend,
  BarElement,
  CategoryScale,
  LinearScale,
} from 'chart.js'
import { Bar } from 'vue-chartjs'

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend)

const props = defineProps({
  data: {
    type: Array,
    required: true
  }
})

const chartData = computed(() => ({
  labels: props.data.map(item => item.uf),
  datasets: [{
    label: 'Expenses by UF (R$ Billion)',
    backgroundColor: '#3b82f6', // bg-blue-500
    data: props.data.map(item => item.total_expenses / 1000000000) // Convert to Billions
  }]
}))

const chartOptions = {
  responsive: true,
  indexAxis: 'y', // Horizontal bars
  plugins: {
    legend: { display: false },
    title: {
      display: true,
      text: 'Distribution of Expenses by State (UF)'
    }
  }
}
</script>

<template>
  <div class="bg-white p-6 rounded-lg shadow-sm">
    <Bar :data="chartData" :options="chartOptions" />
  </div>
</template>
